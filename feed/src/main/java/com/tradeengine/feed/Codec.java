package com.tradeengine.feed;

import com.tradeengine.orderbook.Side;
import java.nio.ByteBuffer;

/**
 * Byte layout of the feed (docs/lld-feed.html, diagram 2). Every field sits
 * at a fixed offset, so encoding and decoding are single absolute reads and
 * writes: no parsing, no strings, no objects.
 *
 * <p>All buffers passed here must be {@link java.nio.ByteOrder#LITTLE_ENDIAN}.
 */
public final class Codec {

    private Codec() { }

    // Packet header: [seqNo u64][count u16][reserved u16]
    public static final int HEADER_SIZE = 12;
    static final int HDR_SEQ = 0;
    static final int HDR_COUNT = 8;

    public static final byte ADD = 'A';
    public static final byte EXECUTE = 'E';
    public static final byte DELETE = 'D';

    // AddOrder: [type][side][orderId u64][price i64][qty u64]
    public static final int ADD_SIZE = 26;
    // OrderExecuted: [type][orderId u64][qty u64][price i64]
    public static final int EXECUTE_SIZE = 25;
    // OrderDeleted: [type][orderId u64]
    public static final int DELETE_SIZE = 9;

    public static final int MAX_MESSAGE_SIZE = ADD_SIZE;

    private static final byte BUY = 'B';
    private static final byte SELL = 'S';

    // --- header ---

    static void writeHeader(ByteBuffer b, long seqNo, int count) {
        b.putLong(HDR_SEQ, seqNo);
        b.putShort(HDR_COUNT, (short) count);
        b.putShort(HDR_COUNT + 2, (short) 0);
    }

    public static long seqNo(ByteBuffer packet) {
        return packet.getLong(HDR_SEQ);
    }

    public static int count(ByteBuffer packet) {
        return packet.getShort(HDR_COUNT) & 0xFFFF;
    }

    // --- encode: each returns the bytes written ---

    static int encodeAdd(ByteBuffer b, int at, long orderId, Side side, long price, long qty) {
        b.put(at, ADD);
        b.put(at + 1, side == Side.BUY ? BUY : SELL);
        b.putLong(at + 2, orderId);
        b.putLong(at + 10, price);
        b.putLong(at + 18, qty);
        return ADD_SIZE;
    }

    static int encodeExecute(ByteBuffer b, int at, long orderId, long qty, long price) {
        b.put(at, EXECUTE);
        b.putLong(at + 1, orderId);
        b.putLong(at + 9, qty);
        b.putLong(at + 17, price);
        return EXECUTE_SIZE;
    }

    static int encodeDelete(ByteBuffer b, int at, long orderId) {
        b.put(at, DELETE);
        b.putLong(at + 1, orderId);
        return DELETE_SIZE;
    }

    // --- decode ---

    public static byte type(ByteBuffer b, int at) {
        return b.get(at);
    }

    /** Size of the message starting at {@code at}, from its type byte. */
    public static int size(ByteBuffer b, int at) {
        return switch (b.get(at)) {
            case ADD -> ADD_SIZE;
            case EXECUTE -> EXECUTE_SIZE;
            case DELETE -> DELETE_SIZE;
            default -> throw new IllegalArgumentException("unknown message type " + b.get(at) + " at " + at);
        };
    }

    /** orderId is at offset 2 in AddOrder and offset 1 in the others. */
    public static long orderId(ByteBuffer b, int at) {
        return b.getLong(at + (b.get(at) == ADD ? 2 : 1));
    }

    public static Side addSide(ByteBuffer b, int at) {
        return b.get(at + 1) == BUY ? Side.BUY : Side.SELL;
    }

    public static long addPrice(ByteBuffer b, int at) { return b.getLong(at + 10); }
    public static long addQty(ByteBuffer b, int at) { return b.getLong(at + 18); }
    public static long executeQty(ByteBuffer b, int at) { return b.getLong(at + 9); }
    public static long executePrice(ByteBuffer b, int at) { return b.getLong(at + 17); }
}
