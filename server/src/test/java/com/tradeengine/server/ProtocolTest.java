package com.tradeengine.server;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.tradeengine.orderbook.OrderType;
import com.tradeengine.orderbook.Side;
import com.tradeengine.raft.LogEntry;
import com.tradeengine.raft.Message;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Every message decodes back to exactly what was encoded. */
class ProtocolTest {

    /** Encodes with {@code write}, sends through a real reader, returns the frames. */
    private static List<ByteBuffer> roundTrip(java.util.function.Consumer<FrameWriter> write) throws IOException {
        FrameWriter w = new FrameWriter(1 << 20);
        write.accept(w);
        FramingTest.SlowChannel sink = new FramingTest.SlowChannel(Integer.MAX_VALUE);
        w.writeTo(sink);
        FrameReader r = new FrameReader(1 << 20);
        FramingTest.ChunkedChannel in = new FramingTest.ChunkedChannel(sink.out.toByteArray(), 5);
        List<ByteBuffer> frames = new ArrayList<>();
        while (r.readFrom(in) >= 0) {
            r.drain((buf, at, len) -> frames.add(copy(buf, at, len)));
        }
        return frames;
    }

    private static ByteBuffer copy(ByteBuffer buf, int at, int len) {
        ByteBuffer c = ByteBuffer.allocate(len).order(buf.order());
        c.put(0, buf, at, len);
        return c;
    }

    @Test
    void clientMessages() throws IOException {
        List<ByteBuffer> f = roundTrip(w -> {
            ClientProtocol.logon(w, 42);
            ClientProtocol.newOrder(w, 9_000_000_001L, Side.SELL, OrderType.IOC, 100_050, 700, 123_456_789L);
            ClientProtocol.cancel(w, 77, 88);
            ClientProtocol.ack(w, 77, 5, 999);
            ClientProtocol.fill(w, 5, 100_051, 30);
            ClientProtocol.cancelled(w, 5, 12);
            ClientProtocol.reject(w, 78, ClientProtocol.RISK_PRICE_COLLAR);
            ClientProtocol.notLeader(w, 2);
        });
        assertEquals(8, f.size());

        assertEquals(42, ClientProtocol.logonClientId(f.get(0), 0));

        ByteBuffer n = f.get(1);
        assertEquals(ClientProtocol.NEW_ORDER, ClientProtocol.type(n, 0));
        assertEquals(9_000_000_001L, ClientProtocol.clOrdId(n, 0));
        assertEquals(Side.SELL, ClientProtocol.side(n, 0));
        assertEquals(OrderType.IOC, ClientProtocol.orderType(n, 0));
        assertEquals(100_050, ClientProtocol.price(n, 0));
        assertEquals(700, ClientProtocol.qty(n, 0));
        assertEquals(123_456_789L, ClientProtocol.sendNanos(n, 0));

        assertEquals(77, ClientProtocol.clOrdId(f.get(2), 0));
        assertEquals(88, ClientProtocol.cancelOrderId(f.get(2), 0));

        assertEquals(5, ClientProtocol.ackOrderId(f.get(3), 0));
        assertEquals(999, ClientProtocol.ackSendNanos(f.get(3), 0));

        assertEquals(100_051, ClientProtocol.fillPrice(f.get(4), 0));
        assertEquals(30, ClientProtocol.fillQty(f.get(4), 0));

        assertEquals(12, ClientProtocol.cancelledLeaves(f.get(5), 0));
        assertEquals(ClientProtocol.RISK_PRICE_COLLAR, ClientProtocol.rejectReason(f.get(6), 0));
        assertEquals(2, ClientProtocol.notLeaderId(f.get(7), 0));
    }

    @Test
    void raftMessages() throws IOException {
        List<LogEntry> entries = List.of(
                new LogEntry(3, new byte[] {1, 2, 3}),
                new LogEntry(3, new byte[0]),                   // no-op
                new LogEntry(4, new byte[39]));
        List<Message> sent = List.of(
                new Message.RequestVote(0, 2, 5, 100, 4),
                new Message.VoteResponse(2, 0, 5, true),
                new Message.AppendEntries(0, 1, 5, 99, 4, entries, 97),
                new Message.AppendEntries(0, 2, 5, 102, 4, List.of(), 102),   // heartbeat
                new Message.AppendResponse(1, 0, 5, false, 42));
        List<ByteBuffer> f = roundTrip(w -> sent.forEach(m -> PeerProtocol.encode(w, m)));

        assertEquals(sent.size(), f.size());
        for (int i = 0; i < sent.size(); i++) {
            Message got = PeerProtocol.decode(f.get(i), 0, f.get(i).capacity());
            Message want = sent.get(i);
            if (want instanceof Message.AppendEntries ae) {
                Message.AppendEntries g = (Message.AppendEntries) got;
                assertEquals(ae.prevLogIndex(), g.prevLogIndex());
                assertEquals(ae.leaderCommit(), g.leaderCommit());
                assertEquals(ae.entries().size(), g.entries().size());
                for (int k = 0; k < ae.entries().size(); k++) {
                    assertEquals(ae.entries().get(k).term(), g.entries().get(k).term());
                    assertArrayEquals(ae.entries().get(k).command(), g.entries().get(k).command());
                }
            } else {
                assertEquals(want, got);                 // records compare by value
            }
        }
    }

    @Test
    void hello() throws IOException {
        List<ByteBuffer> f = roundTrip(w -> PeerProtocol.hello(w, 2));
        assertEquals(PeerProtocol.HELLO, f.get(0).get(0));
        assertEquals(2, PeerProtocol.helloNodeId(f.get(0), 0));
    }
}
