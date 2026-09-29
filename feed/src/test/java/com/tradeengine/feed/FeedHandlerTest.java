package com.tradeengine.feed;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.tradeengine.orderbook.BookListener;
import com.tradeengine.orderbook.Side;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The three cases from docs/lld-feed.html diagram 3, plus unrecoverable loss. */
class FeedHandlerTest {

    private CapturingSink sink;
    private RetransmitStore store;
    private Packetizer packetizer;
    private final List<String> applied = new ArrayList<>();
    private final BookListener recorder = new BookListener() {
        @Override public void onAdd(long id, Side side, long price, long qty) { applied.add("A" + id); }
        @Override public void onExecute(long id, long qty, long price) { applied.add("E" + id); }
        @Override public void onDelete(long id) { applied.add("D" + id); }
    };

    @BeforeEach
    void setUp() {
        sink = new CapturingSink();
        store = new RetransmitStore(64);
        packetizer = new Packetizer(1400, sink, store);
    }

    /** Packet k (0-based) holds orders with ids 10k+1 .. 10k+n as adds. */
    private void sendPackets(int packets, int perPacket) {
        for (int k = 0; k < packets; k++) {
            for (int i = 1; i <= perPacket; i++) {
                packetizer.add(10L * k + i, Side.BUY, 100_000, 1);
            }
            packetizer.flush();
        }
    }

    @Test
    void inOrderPacketsAreApplied() {
        sendPackets(3, 2);
        FeedHandler h = new FeedHandler(recorder, store);
        sink.packets.forEach(h::onPacket);

        assertEquals(List.of("A1", "A2", "A11", "A12", "A21", "A22"), applied);
        assertEquals(7, h.expected());
        assertEquals(0, h.gaps());
    }

    @Test
    void gapIsRecoveredBeforeTheNewPacketIsApplied() {
        sendPackets(3, 2);
        FeedHandler h = new FeedHandler(recorder, store);
        h.onPacket(sink.packets.get(0));
        // packet 1 (seq 3-4) is lost on the network
        h.onPacket(sink.packets.get(2));

        assertEquals(List.of("A1", "A2", "A11", "A12", "A21", "A22"), applied);
        assertEquals(1, h.gaps());
        assertEquals(2, h.recovered());
        assertEquals(0, h.lost());
    }

    @Test
    void duplicateAndOverlappingPacketsApplyOnlyNewMessages() {
        sendPackets(2, 3);
        FeedHandler h = new FeedHandler(recorder, store);
        h.onPacket(sink.packets.get(0));
        h.onPacket(sink.packets.get(0));    // exact duplicate
        h.onPacket(sink.packets.get(1));
        h.onPacket(sink.packets.get(0));    // late copy

        assertEquals(List.of("A1", "A2", "A3", "A11", "A12", "A13"), applied);
        assertEquals(6, h.duplicates());
        assertEquals(0, h.gaps());
    }

    @Test
    void gapTooOldToRecoverIsCountedAsLost() {
        RetransmitStore tiny = new RetransmitStore(2);  // remembers only 2 messages
        packetizer = new Packetizer(1400, sink, tiny);
        sendPackets(4, 2);
        FeedHandler h = new FeedHandler(recorder, tiny);
        h.onPacket(sink.packets.get(0));
        h.onPacket(sink.packets.get(3));    // missing seq 3-6, store only has 7-8

        assertEquals(1, h.gaps());
        assertEquals(4, h.lost());
        assertEquals(List.of("A1", "A2", "A31", "A32"), applied);
        assertEquals(9, h.expected());
    }

    @Test
    void withoutRecoveryAGapIsSkipped() {
        sendPackets(3, 1);
        FeedHandler h = new FeedHandler(recorder, null);
        h.onPacket(sink.packets.get(0));
        h.onPacket(sink.packets.get(2));

        assertEquals(List.of("A1", "A21"), applied);
        assertEquals(1, h.lost());
    }
}
