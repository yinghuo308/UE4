using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace UeNet
{
    internal enum PacketKind : byte { Hello = 1, Welcome, Ack, Reliable, State, Rpc, Ping }
    internal enum MessageKind : byte { Spawn = 1, Despawn, Rpc, Ownership }

    internal static class Wire
    {
        internal const uint Magic = 0x55454E54;
        internal const byte Version = 1;
        internal const int HeaderBytes = 22;
        internal static byte[] Build(PacketKind kind, ulong token, ulong sequence, Action<BinaryWriter> payload)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
            {
                writer.Write(Magic); writer.Write(Version); writer.Write((byte)kind);
                writer.Write(token); writer.Write(sequence); payload?.Invoke(writer); return stream.ToArray();
            }
        }
        internal static byte[] Message(Action<BinaryWriter> payload)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
            { payload(writer); return stream.ToArray(); }
        }
        internal static BinaryReader Reader(byte[] bytes) { return new BinaryReader(new MemoryStream(bytes, false), Encoding.UTF8); }
    }

    internal sealed class Peer
    {
        private sealed class ReliableFrame { internal byte[] Payload; internal Action Acked; }
        private sealed class Pending { internal byte[] Packet; internal double SentAt; internal Action Acked; }
        private sealed class Receipt { internal double SentAt; internal Action Acked; }
        internal readonly string Address;
        internal readonly uint ClientId;
        internal readonly ulong Token;
        internal readonly Dictionary<uint, ServerReplica> Replicas = new Dictionary<uint, ServerReplica>();
        private readonly NetNode node;
        private readonly Queue<ReliableFrame> queue = new Queue<ReliableFrame>();
        private readonly Dictionary<ulong, Pending> pending = new Dictionary<ulong, Pending>();
        private readonly Dictionary<ulong, Receipt> receipts = new Dictionary<ulong, Receipt>();
        private readonly SortedDictionary<ulong, byte[]> incoming = new SortedDictionary<ulong, byte[]>();
        private ulong nextPacket = 1, nextReliable = 1, expectedReliable = 1, messageId = 1;
        private ulong assemblingId;
        private int assemblingIndex, assemblingCount;
        private MemoryStream assembling;
        internal double LastHeard, LastPing;
        internal bool Failed;
        internal int PendingCount => pending.Count + queue.Count;
        internal Peer(NetNode node, string address, uint clientId, ulong token, double now)
        { this.node = node; Address = address; ClientId = clientId; Token = token; LastHeard = LastPing = now; }

        internal void SendReliable(byte[] message, Action acked = null)
        {
            if (message.Length > node.Config.MaxMessageBytes) throw new InvalidOperationException("Reliable message exceeds MaxMessageBytes.");
            int chunkSize = node.Config.MaxDatagramBytes - Wire.HeaderBytes - 20;
            int count = Math.Max(1, (message.Length + chunkSize - 1) / chunkSize);
            if (queue.Count + pending.Count + count > node.Config.MaxQueuedReliableFragments)
                throw new InvalidOperationException("Reliable send queue is full.");
            ulong id = messageId++;
            int remaining = count;
            Action fragmentAcked = () => { if (--remaining == 0) acked?.Invoke(); };
            for (int index = 0; index < count; index++)
            {
                int offset = index * chunkSize, length = Math.Min(chunkSize, message.Length - offset);
                ulong reliableSequence = nextReliable++;
                int fragmentIndex = index;
                queue.Enqueue(new ReliableFrame { Acked = fragmentAcked, Payload = Wire.Message(writer =>
                {
                    writer.Write(reliableSequence); writer.Write(id); writer.Write((ushort)fragmentIndex);
                    writer.Write((ushort)count); writer.Write(message, offset, length);
                }) });
            }
        }

        internal void Send(PacketKind kind, byte[] payload, double now, Action acked = null)
        {
            ulong sequence = nextPacket++;
            byte[] packet = Wire.Build(kind, Token, sequence, writer => writer.Write(payload));
            if (packet.Length > node.Config.MaxDatagramBytes) throw new InvalidOperationException("Datagram exceeds MTU budget.");
            if (acked != null)
            {
                if (receipts.Count >= 8192) throw new InvalidOperationException("State receipt window is full.");
                receipts.Add(sequence, new Receipt { SentAt = now, Acked = acked });
            }
            node.Transmit(Address, packet);
        }

        internal void Ack(ulong sequence) { node.Transmit(Address, Wire.Build(PacketKind.Ack, Token, 0, w => w.Write(sequence))); }
        internal void ReceiveAck(ulong sequence)
        {
            if (pending.TryGetValue(sequence, out var frame)) { pending.Remove(sequence); frame.Acked?.Invoke(); }
            if (receipts.TryGetValue(sequence, out var receipt)) { receipts.Remove(sequence); receipt.Acked(); }
        }
        internal void ReceiveReliable(ulong packetSequence, BinaryReader reader, double now)
        {
            ulong sequence = reader.ReadUInt64();
            var payload = reader.ReadBytes((int)(reader.BaseStream.Length - reader.BaseStream.Position));
            if (sequence < expectedReliable) { Ack(packetSequence); return; }
            if (sequence - expectedReliable >= 256) return; // Do not ACK data outside our bounded receive window.
            if (!incoming.ContainsKey(sequence)) incoming.Add(sequence, payload);
            Ack(packetSequence);
            while (incoming.TryGetValue(expectedReliable, out var fragment))
            {
                incoming.Remove(expectedReliable++);
                using (var part = Wire.Reader(fragment))
                {
                    ulong id = part.ReadUInt64(); int index = part.ReadUInt16(), count = part.ReadUInt16();
                    if (count == 0 || count > 128 || index >= count) throw new InvalidDataException("Invalid fragment header.");
                    if (index == 0)
                    {
                        if (assembling != null) throw new InvalidDataException("Interleaved fragment stream.");
                        assembling = new MemoryStream(); assemblingId = id; assemblingIndex = 0; assemblingCount = count;
                    }
                    if (assembling == null || assemblingId != id || assemblingIndex != index || assemblingCount != count)
                        throw new InvalidDataException("Invalid fragment order.");
                    var bytes = part.ReadBytes((int)(part.BaseStream.Length - part.BaseStream.Position));
                    if (assembling.Length + bytes.Length > node.Config.MaxMessageBytes) throw new InvalidDataException("Message too large.");
                    assembling.Write(bytes, 0, bytes.Length); assemblingIndex++;
                    if (assemblingIndex == count)
                    {
                        var message = assembling.ToArray(); assembling.Dispose(); assembling = null;
                        node.ReceiveMessage(this, message, true, now);
                    }
                }
            }
        }
        internal void Tick(double now)
        {
            foreach (var item in pending.Values)
            {
                if (now - item.SentAt < node.Config.RetrySeconds) continue;
                item.SentAt = now; node.Stats.Retransmissions++; node.Transmit(Address, item.Packet);
            }
            while (pending.Count < node.Config.ReliableWindow && queue.Count > 0)
            {
                var frame = queue.Dequeue(); ulong sequence = nextPacket++;
                byte[] packet = Wire.Build(PacketKind.Reliable, Token, sequence, w => w.Write(frame.Payload));
                pending.Add(sequence, new Pending { Packet = packet, SentAt = now, Acked = frame.Acked });
                node.Transmit(Address, packet);
            }
            var expired = new List<ulong>();
            foreach (var item in receipts) if (now - item.Value.SentAt > 3) expired.Add(item.Key);
            foreach (ulong id in expired) receipts.Remove(id);
            if (now - LastPing > 1) { Send(PacketKind.Ping, Array.Empty<byte>(), now); LastPing = now; }
            if (now - LastHeard > node.Config.PeerTimeoutSeconds) Failed = true;
        }
    }
}
