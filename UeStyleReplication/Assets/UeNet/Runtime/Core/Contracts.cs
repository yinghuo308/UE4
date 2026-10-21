using System;
using System.Collections.Generic;

namespace UeNet
{
    public enum RepCondition { Everyone, OwnerOnly, SkipOwner, InitialOnly, InitialOrOwner }
    public enum RpcDirection { Server, Client, Multicast }

    [AttributeUsage(AttributeTargets.Field)]
    public sealed class ReplicatedAttribute : Attribute
    {
        public ushort Id { get; }
        public RepCondition Condition { get; set; }
        public string Notify { get; set; }
        public ReplicatedAttribute(ushort id) { Id = id; }
    }

    [AttributeUsage(AttributeTargets.Method)]
    public sealed class NetRpcAttribute : Attribute
    {
        public ushort Id { get; }
        public RpcDirection Direction { get; }
        public bool Reliable { get; set; } = true;
        public bool RequireOwner { get; set; } = true;
        public NetRpcAttribute(ushort id, RpcDirection direction) { Id = id; Direction = direction; }
    }

    public abstract class NetEntity
    {
        public uint NetworkId { get; internal set; }
        public ushort PrefabId { get; internal set; }
        public uint OwnerId { get; internal set; }
        public uint OuterId { get; internal set; }
        public NetNode Node { get; internal set; }
        public bool Dormant { get; set; }
        public float UpdateInterval { get; set; } = 0.05f;
        public virtual void OnNetworkSpawn() { }
        public virtual void OnNetworkDespawn() { }
        public void CallRpc(ushort methodId, params object[] arguments)
        {
            if (Node == null) throw new InvalidOperationException("Entity is not spawned.");
            Node.SendRpc(this, methodId, arguments);
        }
    }

    public sealed class NetConfig
    {
        public double RetrySeconds = 0.12;
        public double StateRetrySeconds = 0.15;
        public double PeerTimeoutSeconds = 15;
        public double RpcDependencyTimeoutSeconds = 10;
        public int MaxDatagramBytes = 1200;
        public int ReliableWindow = 48;
        public int MaxQueuedReliableFragments = 4096;
        public int MaxMessageBytes = 65536;
        public int MaxPendingRpcs = 256;
        public int MaxEntities = 4096;
        public int MaxPeers = 32;
        public int ReceiveBudget = 2048;
        public bool DelayUnmappedReliableRpcs = true;
    }

    public sealed class NetStats
    {
        public long SentPackets, ReceivedPackets, SentBytes, ReceivedBytes;
        public long Retransmissions, StateFrames, StateFields, RejectedPackets, RejectedRpcs;
        public long FieldComparisons, ReferenceRepairs, RpcExecutions;
        public int PendingReliable, UnmappedFields, PendingRpcs;
    }

    public readonly struct PeerInfo
    {
        public readonly uint ClientId;
        public readonly string Address;
        public PeerInfo(uint id, string address) { ClientId = id; Address = address; }
    }
}
