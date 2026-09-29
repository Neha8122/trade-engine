package com.tradeengine.feed;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * The last N messages the exchange sent, kept by sequence number so a
 * subscriber that lost packets can fetch exactly what it missed.
 *
 * <p>One flat byte array with a fixed-size slot per message (every message
 * fits in 32 bytes): slot = seqNo &amp; (N - 1). Newer messages overwrite older
 * ones, so it covers short gaps; a longer outage needs a snapshot.
 */
public final class RetransmitStore implements Recovery {

    private static final int SLOT = 32;

    private final ByteBuffer bytes;
    private final long[] seqs;      // which seqNo each slot holds, 0 = none
    private final int mask;

    public RetransmitStore(int capacity) {
        if (capacity <= 0 || Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException("capacity must be a power of 2: " + capacity);
        }
        bytes = ByteBuffer.allocateDirect(capacity * SLOT).order(ByteOrder.LITTLE_ENDIAN);
        seqs = new long[capacity];
        mask = capacity - 1;
    }

    /** Copies one encoded message out of {@code src}. */
    void store(long seqNo, ByteBuffer src, int offset, int length) {
        int slot = (int) (seqNo & mask);
        bytes.put(slot * SLOT, src, offset, length);
        seqs[slot] = seqNo;
    }

    @Override
    public boolean replay(long from, long toInclusive, MessageHandler handler) {
        // Check the whole range first: all or nothing.
        for (long s = from; s <= toInclusive; s++) {
            if (seqs[(int) (s & mask)] != s) {
                return false;
            }
        }
        for (long s = from; s <= toInclusive; s++) {
            handler.onMessage(bytes, (int) (s & mask) * SLOT, s);
        }
        return true;
    }

    public int capacity() {
        return seqs.length;
    }
}
