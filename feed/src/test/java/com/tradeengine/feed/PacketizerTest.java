package com.tradeengine.feed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradeengine.orderbook.Side;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class PacketizerTest {

    private final CapturingSink sink = new CapturingSink();

    @Test
    void flushSendsOnePacketWithAllMessages() {
        Packetizer p = new Packetizer(1400, sink, null);
        p.add(1, Side.BUY, 100_040, 10);
        p.execute(1, 4, 100_040);
        p.delete(1);
        p.flush();

        assertEquals(1, sink.packets.size());
        ByteBuffer pkt = sink.packets.get(0);
        assertEquals(1, Codec.seqNo(pkt));
        assertEquals(3, Codec.count(pkt));
        assertEquals(Codec.HEADER_SIZE + 26 + 25 + 9, pkt.remaining());
    }

    @Test
    void emptyFlushSendsNothing() {
        Packetizer p = new Packetizer(1400, sink, null);
        p.flush();
        assertEquals(0, sink.packets.size());
    }

    @Test
    void neverExceedsMaxSizeAndSeqNosAreContinuous() {
        int max = 100;                  // header + 3 adds (12 + 78 = 90) fits, 4 doesn't
        Packetizer p = new Packetizer(max, sink, null);
        for (int i = 1; i <= 10; i++) {
            p.add(i, Side.SELL, 100_050, i);
        }
        p.flush();

        long expectedSeq = 1;
        for (ByteBuffer pkt : sink.packets) {
            assertTrue(pkt.remaining() <= max, "packet too big: " + pkt.remaining());
            assertEquals(expectedSeq, Codec.seqNo(pkt));
            expectedSeq += Codec.count(pkt);
        }
        assertEquals(11, expectedSeq);
        assertEquals(4, sink.packets.size());       // 3 + 3 + 3 + 1
        assertEquals(11, p.nextSeq());
    }

    @Test
    void storeKeepsEveryMessageForReplay() {
        RetransmitStore store = new RetransmitStore(16);
        Packetizer p = new Packetizer(1400, sink, store);
        p.add(7, Side.BUY, 100_010, 5);
        p.delete(7);
        p.flush();

        StringBuilder seen = new StringBuilder();
        assertTrue(store.replay(1, 2, (buf, at, seq) ->
                seen.append(seq).append((char) Codec.type(buf, at)).append(Codec.orderId(buf, at)).append(' ')));
        assertEquals("1A7 2D7 ", seen.toString());
    }

    @Test
    void rejectsMaxSmallerThanOneMessage() {
        assertThrows(IllegalArgumentException.class, () -> new Packetizer(30, sink, null));
    }
}
