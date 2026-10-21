using System;
using System.Diagnostics;
using System.Threading;
using UeNet;

internal static class Program
{
    private static void Main()
    {
        using (var server = new UdpTransport(0, "127.0.0.1"))
        using (var first = new UdpTransport(0, "127.0.0.1"))
        using (var second = new UdpTransport(0, "127.0.0.1"))
        {
            var timer = Stopwatch.StartNew();
            Assert(!server.TryReceive(out _), "Empty socket must return false.");
            Assert(timer.ElapsedMilliseconds < 500, "Empty receive must not block.");
            Console.WriteLine("PASS: nonblocking empty receive");

            first.Send(server.LocalAddress, new byte[] { 1, 2, 3 });
            var request = Receive(server);
            Assert(request.Remote == first.LocalAddress, "Sender address must be preserved.");
            Assert(request.Bytes.Length == 3 && request.Bytes[2] == 3, "Request bytes differ.");
            server.Send(request.Remote, new byte[] { 9 });
            Assert(Receive(first).Bytes[0] == 9, "Server reply differs.");
            Console.WriteLine("PASS: bidirectional UDP loopback");

            second.Send(server.LocalAddress, new byte[] { 4 });
            var other = Receive(server);
            Assert(other.Remote == second.LocalAddress && other.Remote != request.Remote,
                "Different clients must retain different endpoints.");
            Assert(request.Bytes.Length == 3 && request.Bytes[0] == 1,
                "Later receives must not overwrite previously returned data.");
            Console.WriteLine("PASS: multiple clients and independent receive buffers");

            first.Send(server.LocalAddress, Array.Empty<byte>());
            Assert(Receive(server).Bytes.Length == 0, "Zero-length datagrams must be returned.");
            var payload = new byte[1200];
            for (int i = 0; i < payload.Length; i++) payload[i] = (byte)i;
            first.Send(server.LocalAddress, payload);
            var received = Receive(server).Bytes;
            Assert(received.Length == payload.Length, "1200-byte payload was truncated.");
            for (int i = 0; i < payload.Length; i++)
                Assert(received[i] == payload[i], "Payload content differs.");
            Console.WriteLine("PASS: zero-length and 1200-byte datagrams");

            foreach (string address in new[] { "", "localhost:7777", "127.0.0.1", "127.0.0.1:0",
                "127.0.0.1:65536", "127.0.0.1:-1", "[::1]:7777" })
                Throws<ArgumentException>(() => first.Send(address, new byte[1]));
            Throws<ArgumentNullException>(() => first.Send(null, new byte[1]));
            Throws<ArgumentNullException>(() => first.Send(server.LocalAddress, null));
            Throws<ArgumentException>(() => first.Send(server.LocalAddress,
                new byte[UdpTransport.MaxPayloadBytes + 1]));
            Throws<ArgumentOutOfRangeException>(() => new UdpTransport(-1));
            Throws<ArgumentOutOfRangeException>(() => new UdpTransport(65536));
            Throws<ArgumentException>(() => new UdpTransport(0, "::1"));
            Console.WriteLine("PASS: invalid arguments and oversized sends rejected");
        }

        var closed = new UdpTransport(0, "127.0.0.1");
        int releasedPort = int.Parse(closed.LocalAddress.Substring(closed.LocalAddress.LastIndexOf(':') + 1));
        closed.Dispose();
        closed.Dispose();
        Throws<ObjectDisposedException>(() => closed.TryReceive(out _));
        Throws<ObjectDisposedException>(() => closed.Send("127.0.0.1:7777", new byte[1]));
        Throws<ObjectDisposedException>(() => { string ignored = closed.LocalAddress; });
        using (var rebound = new UdpTransport(releasedPort, "127.0.0.1"))
            Assert(rebound.LocalAddress.EndsWith(":" + releasedPort), "Disposed port was not released.");
        Console.WriteLine("PASS: idempotent disposal and port release");
    }

    private static Datagram Receive(ITransport transport)
    {
        var timer = Stopwatch.StartNew();
        while (timer.ElapsedMilliseconds < 2000)
        {
            if (transport.TryReceive(out var datagram)) return datagram;
            Thread.Sleep(1);
        }
        throw new Exception("Timed out waiting for a loopback datagram.");
    }

    private static void Assert(bool condition, string message)
    {
        if (!condition) throw new Exception(message);
    }

    private static void Throws<T>(Action action) where T : Exception
    {
        try { action(); }
        catch (T) { return; }
        throw new Exception("Expected " + typeof(T).Name);
    }
}
