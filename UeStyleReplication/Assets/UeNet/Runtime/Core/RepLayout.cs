using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace UeNet
{
    public sealed class RepField
    {
        public readonly FieldInfo Field;
        public readonly ReplicatedAttribute Rule;
        internal readonly MethodInfo Notify;
        internal RepField(FieldInfo field, ReplicatedAttribute rule)
        {
            Field = field; Rule = rule;
            Codec.Validate(field.FieldType);
            if (field.IsInitOnly) throw new InvalidOperationException("Replicated fields cannot be readonly.");
            if (!string.IsNullOrEmpty(rule.Notify))
            {
                Notify = field.DeclaringType.GetMethod(rule.Notify, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                    null, Type.EmptyTypes, null);
                if (Notify == null || Notify.ReturnType != typeof(void))
                    throw new InvalidOperationException("RepNotify must be a parameterless void method: " + rule.Notify);
            }
        }
        internal bool Allows(uint owner, uint recipient, bool initial)
        {
            switch (Rule.Condition)
            {
                case RepCondition.OwnerOnly: return owner == recipient;
                case RepCondition.SkipOwner: return owner != recipient;
                case RepCondition.InitialOnly: return initial;
                case RepCondition.InitialOrOwner: return initial || owner == recipient;
                default: return true;
            }
        }
    }

    internal sealed class RpcLayout
    {
        internal readonly MethodInfo Method;
        internal readonly NetRpcAttribute Rule;
        internal readonly ParameterInfo[] Parameters;
        internal RpcLayout(MethodInfo method, NetRpcAttribute rule)
        {
            Method = method; Rule = rule; Parameters = method.GetParameters();
            if (method.ReturnType != typeof(void) || method.IsGenericMethod)
                throw new InvalidOperationException("RPCs must be non-generic void methods.");
            foreach (var parameter in Parameters) Codec.Validate(parameter.ParameterType);
        }
    }

    public sealed class RepLayout
    {
        private static readonly Dictionary<Type, RepLayout> Cache = new Dictionary<Type, RepLayout>();
        public readonly RepField[] Fields;
        internal readonly Dictionary<ushort, RepField> FieldById;
        internal readonly Dictionary<ushort, RpcLayout> Rpcs;
        public static RepLayout For(Type type)
        {
            if (!Cache.TryGetValue(type, out var layout)) Cache[type] = layout = new RepLayout(type);
            return layout;
        }
        private RepLayout(Type type)
        {
            var fields = new List<RepField>();
            Rpcs = new Dictionary<ushort, RpcLayout>();
            for (var current = type; current != null && current != typeof(NetEntity); current = current.BaseType)
            {
                const BindingFlags flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly;
                foreach (var field in current.GetFields(flags))
                {
                    var rule = field.GetCustomAttribute<ReplicatedAttribute>();
                    if (rule != null) fields.Add(new RepField(field, rule));
                }
                foreach (var method in current.GetMethods(flags))
                {
                    var rule = method.GetCustomAttribute<NetRpcAttribute>();
                    if (rule == null) continue;
                    if (Rpcs.ContainsKey(rule.Id)) throw new InvalidOperationException("Duplicate RPC id: " + rule.Id);
                    Rpcs.Add(rule.Id, new RpcLayout(method, rule));
                }
            }
            Fields = fields.OrderBy(f => f.Rule.Id).ToArray();
            FieldById = new Dictionary<ushort, RepField>();
            foreach (var field in Fields)
            {
                if (FieldById.ContainsKey(field.Rule.Id)) throw new InvalidOperationException("Duplicate property id: " + field.Rule.Id);
                FieldById.Add(field.Rule.Id, field);
            }
        }
    }

    internal static class Codec
    {
        internal static void Validate(Type type)
        {
            if (type == typeof(bool) || type == typeof(byte) || type == typeof(short) || type == typeof(ushort) ||
                type == typeof(int) || type == typeof(uint) || type == typeof(long) || type == typeof(ulong) ||
                type == typeof(float) || type == typeof(double) || type == typeof(string) || type == typeof(byte[]) ||
                typeof(NetEntity).IsAssignableFrom(type)) return;
            throw new NotSupportedException("Unsupported network type: " + type + ". Use primitive fields or explicit serializers.");
        }
        internal static byte[] Encode(Type type, object value)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
            { Write(writer, type, value); return stream.ToArray(); }
        }
        internal static void Write(BinaryWriter writer, Type type, object value)
        {
            if (typeof(NetEntity).IsAssignableFrom(type))
            {
                var entity = (NetEntity)value;
                if (entity != null && entity.NetworkId == 0) throw new InvalidOperationException("Cannot reference an unspawned object.");
                writer.Write(entity == null ? 0u : entity.NetworkId);
            }
            else if (type == typeof(bool)) writer.Write((bool)value);
            else if (type == typeof(byte)) writer.Write((byte)value);
            else if (type == typeof(short)) writer.Write((short)value);
            else if (type == typeof(ushort)) writer.Write((ushort)value);
            else if (type == typeof(int)) writer.Write((int)value);
            else if (type == typeof(uint)) writer.Write((uint)value);
            else if (type == typeof(long)) writer.Write((long)value);
            else if (type == typeof(ulong)) writer.Write((ulong)value);
            else if (type == typeof(float)) writer.Write((float)value);
            else if (type == typeof(double)) writer.Write((double)value);
            else if (type == typeof(string))
            {
                writer.Write(value != null);
                if (value != null) { var bytes = Encoding.UTF8.GetBytes((string)value); WriteBlob(writer, bytes); }
            }
            else if (type == typeof(byte[]))
            { writer.Write(value != null); if (value != null) WriteBlob(writer, (byte[])value); }
            else throw new NotSupportedException(type.ToString());
        }
        internal static object Read(BinaryReader reader, Type type, NetNode node, HashSet<uint> missing)
        {
            if (typeof(NetEntity).IsAssignableFrom(type))
            {
                uint id = reader.ReadUInt32();
                if (id == 0) return null;
                if (!node.Entities.TryGetValue(id, out var entity)) { missing.Add(id); return null; }
                if (!type.IsInstanceOfType(entity)) throw new InvalidDataException("Object reference type mismatch.");
                return entity;
            }
            if (type == typeof(bool)) return reader.ReadBoolean();
            if (type == typeof(byte)) return reader.ReadByte();
            if (type == typeof(short)) return reader.ReadInt16();
            if (type == typeof(ushort)) return reader.ReadUInt16();
            if (type == typeof(int)) return reader.ReadInt32();
            if (type == typeof(uint)) return reader.ReadUInt32();
            if (type == typeof(long)) return reader.ReadInt64();
            if (type == typeof(ulong)) return reader.ReadUInt64();
            if (type == typeof(float)) { float value = reader.ReadSingle(); if (float.IsNaN(value) || float.IsInfinity(value)) throw new InvalidDataException("Non-finite float."); return value; }
            if (type == typeof(double)) { double value = reader.ReadDouble(); if (double.IsNaN(value) || double.IsInfinity(value)) throw new InvalidDataException("Non-finite double."); return value; }
            if (type == typeof(string)) return reader.ReadBoolean() ? Encoding.UTF8.GetString(ReadBlob(reader)) : null;
            if (type == typeof(byte[])) return reader.ReadBoolean() ? ReadBlob(reader) : null;
            throw new InvalidDataException("Unsupported property type.");
        }
        internal static void WriteBlob(BinaryWriter writer, byte[] bytes) { writer.Write(bytes.Length); writer.Write(bytes); }
        internal static byte[] ReadBlob(BinaryReader reader)
        {
            int length = reader.ReadInt32();
            if (length < 0 || length > 65536 || length > reader.BaseStream.Length - reader.BaseStream.Position)
                throw new InvalidDataException("Invalid blob length.");
            return reader.ReadBytes(length);
        }
        internal static bool Equal(byte[] a, byte[] b)
        {
            if (a.Length != b.Length) return false;
            for (int i = 0; i < a.Length; i++) if (a[i] != b[i]) return false;
            return true;
        }
        internal static void RequireEnd(BinaryReader reader)
        { if (reader.BaseStream.Position != reader.BaseStream.Length) throw new InvalidDataException("Trailing bytes."); }
    }
}
