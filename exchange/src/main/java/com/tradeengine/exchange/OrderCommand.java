package com.tradeengine.exchange;

import com.tradeengine.orderbook.OrderBook;
import com.tradeengine.orderbook.OrderType;
import com.tradeengine.orderbook.Side;
import java.nio.ByteBuffer;

/**
 * How an order or cancel is stored in the Raft log: a few fixed-layout
 * bytes. Raft only sees bytes; every node decodes the same bytes and calls
 * the same OrderBook method, which is what keeps the replicas identical.
 *
 * <pre>
 * NEW    [1][clientId i32][clOrdId i64][side][type][price i64][qty i64][timestamp i64]  39 B
 * CANCEL [2][clientId i32][clOrdId i64][orderId i64]                                    21 B
 * </pre>
 *
 * The timestamp is stamped once by the leader when it accepts the order, so
 * every replica uses the same value (docs/DESIGN.md D5: no clocks inside the
 * engine).
 */
public final class OrderCommand {

    private OrderCommand() { }

    static final byte NEW = 1;
    static final byte CANCEL = 2;

    public static byte[] newOrder(int clientId, long clOrdId, Side side, OrderType type,
                                  long price, long qty, long timestamp) {
        return ByteBuffer.allocate(39)
                .put(NEW).putInt(clientId).putLong(clOrdId)
                .put((byte) side.ordinal()).put((byte) type.ordinal())
                .putLong(price).putLong(qty).putLong(timestamp)
                .array();
    }

    public static byte[] cancel(int clientId, long clOrdId, long orderId) {
        return ByteBuffer.allocate(21)
                .put(CANCEL).putInt(clientId).putLong(clOrdId).putLong(orderId)
                .array();
    }

    static int clientId(byte[] cmd) {
        return ByteBuffer.wrap(cmd).getInt(1);
    }

    static long clOrdId(byte[] cmd) {
        return ByteBuffer.wrap(cmd).getLong(5);
    }

    /** Runs the command against the book. */
    static void applyTo(byte[] cmd, OrderBook book) {
        ByteBuffer b = ByteBuffer.wrap(cmd);
        byte kind = b.get(0);
        int clientId = b.getInt(1);
        long clOrdId = b.getLong(5);
        if (kind == NEW) {
            Side side = Side.values()[b.get(13)];
            OrderType type = OrderType.values()[b.get(14)];
            book.newOrder(clientId, clOrdId, side, type, b.getLong(15), b.getLong(23), b.getLong(31));
        } else if (kind == CANCEL) {
            book.cancel(clOrdId, b.getLong(13));
        } else {
            throw new IllegalArgumentException("unknown command " + kind);
        }
    }
}
