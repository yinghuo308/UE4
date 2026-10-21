using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;

namespace UeNet
{
    // 一次 UDP 收包的结果。Bytes 独立持有数据，后续收包不会覆盖它。
    public readonly struct Datagram
    {
        public readonly string Remote;
        public readonly byte[] Bytes;

        public Datagram(string remote, byte[] bytes)
        {
            Remote = remote;
            Bytes = bytes;
        }
    }

    public interface ITransport : IDisposable
    {
        string LocalAddress { get; }
        void Send(string remote, byte[] bytes);
        bool TryReceive(out Datagram datagram);
    }

    /// <summary>
    /// IPv4 UDP 传输层，只负责字节和地址，不负责连接、ACK、重传或对象同步。
    /// 在 NetworkDriver 所在的同一个线程调用；它不是线程安全的。
    /// </summary>
    public sealed class UdpTransport : ITransport
    {
        // IPv4 UDP 的最大负载。上层组包建议控制在 1200 字节以内，避免 IP 分片。
        public const int MaxPayloadBytes = 65507;

        private readonly Socket socket;
        // 足以容纳完整 UDP 数据报，避免小接收缓冲区导致数据被截断。
        private readonly byte[] buffer = new byte[65536];
        private bool disposed;

        public string LocalAddress
        {
            get
            {
                ThrowIfDisposed();
                return socket.LocalEndPoint.ToString();
            }
        }

        /// <param name="port">服务端使用固定端口，客户端传 0，让系统分配端口。</param>
        /// <param name="bindAddress">默认监听所有本地 IPv4 网卡。</param>
        public UdpTransport(int port, string bindAddress = "0.0.0.0")
        {
            if (port < 0 || port > 65535)
                throw new ArgumentOutOfRangeException(nameof(port));
            if (!IPAddress.TryParse(bindAddress, out var address) ||
                address.AddressFamily != AddressFamily.InterNetwork)
                throw new ArgumentException("Bind address must be an IPv4 address.", nameof(bindAddress));

            socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            try
            {
                socket.Bind(new IPEndPoint(address, port));
                // 没有数据时立即返回，避免卡住 Unity 主线程。
                socket.Blocking = false;
            }
            catch
            {
                socket.Dispose();
                throw;
            }
        }

        /// <summary>
        /// 将一个完整数据报交给系统，不代表对方已收到。
        /// 包括发送缓冲区暂满在内的 SocketException 交给上层处理，不静默丢包。
        /// </summary>
        public void Send(string remote, byte[] bytes)
        {
            ThrowIfDisposed();
            if (bytes == null) throw new ArgumentNullException(nameof(bytes));
            if (bytes.Length > MaxPayloadBytes)
                throw new ArgumentException("Payload exceeds the IPv4 UDP limit.", nameof(bytes));

            var endpoint = ParseRemote(remote);
            int sent = socket.SendTo(bytes, endpoint);
            if (sent != bytes.Length)
                throw new InvalidOperationException("The complete UDP datagram was not sent.");
        }

        /// <summary>
        /// 每次最多取出一个数据报；没有数据返回 false。零字节数据报也是有效数据报。
        /// 上层每帧循环调用，并自行限制每帧收包数量。
        /// </summary>
        public bool TryReceive(out Datagram datagram)
        {
            ThrowIfDisposed();
            EndPoint remote = new IPEndPoint(IPAddress.Any, 0);
            try
            {
                int length = socket.ReceiveFrom(buffer, ref remote);
                var bytes = new byte[length];
                Buffer.BlockCopy(buffer, 0, bytes, 0, length);
                datagram = new Datagram(remote.ToString(), bytes);
                return true;
            }
            // Windows 可能将之前发送收到的 ICMP 端口不可达报告为 ConnectionReset。
            // UDP 没有连接关闭事件，是否超时由上层 Connection 判断。
            catch (SocketException exception) when (exception.SocketErrorCode == SocketError.WouldBlock ||
                exception.SocketErrorCode == SocketError.ConnectionReset)
            {
                datagram = default;
                return false;
            }
        }

        private static IPEndPoint ParseRemote(string remote)
        {
            if (remote == null) throw new ArgumentNullException(nameof(remote));
            int colon = remote.LastIndexOf(':');
            if (colon <= 0 ||
                !IPAddress.TryParse(remote.Substring(0, colon), out var address) ||
                address.AddressFamily != AddressFamily.InterNetwork ||
                !ushort.TryParse(remote.Substring(colon + 1),
                    System.Globalization.NumberStyles.None,
                    System.Globalization.CultureInfo.InvariantCulture, out var port) || port == 0)
                throw new ArgumentException("Use an IPv4 endpoint, for example 127.0.0.1:7777.", nameof(remote));

            return new IPEndPoint(address, port);
        }

        private void ThrowIfDisposed()
        {
            if (disposed) throw new ObjectDisposedException(nameof(UdpTransport));
        }

        public void Dispose()
        {
            if (disposed) return;
            disposed = true;
            socket.Dispose();
        }
    }

    // Fault injection is transport-level and works on both real UDP and in-memory tests.
    public sealed class SimulatedTransport : ITransport
    {
        private sealed class Scheduled { internal double Due; internal string Remote; internal byte[] Bytes; }
        private readonly ITransport inner;
        private readonly Random random;
        private readonly List<Scheduled> outgoing = new List<Scheduled>();
        private double now;
        public double Loss { get; set; }
        public double Duplicate { get; set; }
        public double LatencySeconds { get; set; }
        public double JitterSeconds { get; set; }
        public string LocalAddress => inner.LocalAddress;
        public long Dropped { get; private set; }
        public SimulatedTransport(ITransport inner, int seed = 12345) { this.inner = inner; random = new Random(seed); }
        public void Send(string remote, byte[] bytes)
        {
            if (random.NextDouble() < Loss) { Dropped++; return; }
            Schedule(remote, bytes);
            if (random.NextDouble() < Duplicate) Schedule(remote, bytes);
        }
        private void Schedule(string remote, byte[] bytes)
        {
            if (outgoing.Count >= 8192) { Dropped++; return; }
            outgoing.Add(new Scheduled { Remote = remote, Bytes = (byte[])bytes.Clone(),
                Due = now + Math.Max(0, LatencySeconds + (random.NextDouble() * 2 - 1) * JitterSeconds) });
        }
        public void Advance(double time)
        {
            now = time;
            // Reverse iteration deliberately permits packet reordering when due times coincide.
            for (int i = outgoing.Count - 1; i >= 0; i--)
            {
                if (outgoing[i].Due > now) continue;
                var packet = outgoing[i]; outgoing.RemoveAt(i); inner.Send(packet.Remote, packet.Bytes);
            }
        }
        public bool TryReceive(out Datagram datagram) { return inner.TryReceive(out datagram); }
        public void Dispose() { outgoing.Clear(); inner.Dispose(); }
    }
}
