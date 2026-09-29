package com.tradeengine.feed;

import com.tradeengine.orderbook.BookListener;
import java.nio.ByteBuffer;

/**
 * Subscriber side: turns packets back into book events, strictly in
 * sequence order (docs/lld-feed.html, diagram 3).
 *
 * <ul>
 *   <li>packet starts at the expected seqNo → apply it</li>
 *   <li>packet starts later → a gap: fetch the missing messages from
 *       {@link Recovery}, apply them, then apply the packet</li>
 *   <li>packet starts earlier → duplicate or late: skip what was already
 *       applied, apply the rest</li>
 * </ul>
 *
 * Decoded events go to a {@link BookListener}, the same interface the book
 * used on the exchange side, so a replica receives exactly what the
 * publisher saw.
 */
public final class FeedHandler {

    private final BookListener book;
    private final Recovery recovery;    // null: gaps are skipped, data lost
    private final MessageHandler apply = this::apply;

    private long expected = 1;
    private long packets;
    private long gaps;
    private long recovered;
    private long lost;
    private long duplicates;

    public FeedHandler(BookListener book, Recovery recovery) {
        this.book = book;
        this.recovery = recovery;
    }

    /** One received packet, from its start (index 0) to its limit. */
    public void onPacket(ByteBuffer packet) {
        packets++;
        long first = Codec.seqNo(packet);
        int count = Codec.count(packet);

        if (first > expected) {
            gaps++;
            long missing = first - expected;
            if (recovery != null && recovery.replay(expected, first - 1, apply)) {
                recovered += missing;
            } else {
                // Can't recover: the replica is now wrong until a snapshot.
                lost += missing;
                expected = first;
            }
        }

        int offset = Codec.HEADER_SIZE;
        for (int i = 0; i < count; i++) {
            long seq = first + i;
            if (seq < expected) {
                duplicates++;           // already applied
            } else {
                apply(packet, offset, seq);
            }
            offset += Codec.size(packet, offset);
        }
    }

    private void apply(ByteBuffer b, int at, long seq) {
        if (seq != expected) {
            throw new IllegalStateException("out of order: got " + seq + ", expected " + expected);
        }
        switch (Codec.type(b, at)) {
            case Codec.ADD -> book.onAdd(Codec.orderId(b, at), Codec.addSide(b, at),
                    Codec.addPrice(b, at), Codec.addQty(b, at));
            case Codec.EXECUTE -> book.onExecute(Codec.orderId(b, at),
                    Codec.executeQty(b, at), Codec.executePrice(b, at));
            case Codec.DELETE -> book.onDelete(Codec.orderId(b, at));
            default -> throw new IllegalArgumentException("unknown message type at seq " + seq);
        }
        expected++;
    }

    /** Next seqNo this subscriber needs. */
    public long expected() { return expected; }
    public long packets() { return packets; }
    public long gaps() { return gaps; }
    public long recovered() { return recovered; }
    public long lost() { return lost; }
    public long duplicates() { return duplicates; }
}
