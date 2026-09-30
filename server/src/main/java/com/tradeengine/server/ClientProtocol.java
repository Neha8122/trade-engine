package com.tradeengine.server;

import com.tradeengine.orderbook.OrderType;
import com.tradeengine.orderbook.RejectReason;
import com.tradeengine.orderbook.Side;
import java.nio.ByteBuffer;

/**
 * Messages between clients and the exchange (docs/lld-gateway.html §4).
 * Encoders append a frame to a {@link FrameWriter}; decoders read fields at
 * fixed offsets from the frame's type byte, so nothing is parsed or copied.
 */
public final class ClientProtocol {

    private ClientProtocol() { }

    // Client → server
    public static final byte LOGON = 'L';          // clientId i32
    public static final byte NEW_ORDER = 'N';      // clOrdId, side, type, price, qty, sendNanos
    public static final byte CANCEL = 'C';         // clOrdId, orderId

    // Server → client
    public static final byte ACK = 'A';            // clOrdId, orderId, sendNanos
    public static final byte FILL = 'F';           // orderId, price, qty
    public static final byte CANCELLED = 'X';      // orderId, leaves
    public static final byte REJECT = 'R';         // clOrdId, reason
    public static final byte NOT_LEADER = '?';     // leaderId i32, -1 if unknown

    /**
     * Extra reject reasons the gateway adds to the book's own. Carried in the
     * same byte: book reasons use their ordinal, these start at 100.
     */
    public static final int RISK_MAX_QTY = 100;
    public static final int RISK_PRICE_COLLAR = 101;
    public static final int RISK_OPEN_ORDERS = 102;
    public static final int RISK_RATE_LIMIT = 103;
    public static final int NOT_LOGGED_ON = 104;

    public static int reasonCode(RejectReason r) {
        return r.ordinal();
    }

    // --- encode ---

    public static void logon(FrameWriter w, int clientId) {
        w.begin(LOGON, 4).putInt(clientId);
        w.end();
    }

    public static void newOrder(FrameWriter w, long clOrdId, Side side, OrderType type,
                                long price, long qty, long sendNanos) {
        w.begin(NEW_ORDER, 34).putLong(clOrdId).put((byte) side.ordinal()).put((byte) type.ordinal())
                .putLong(price).putLong(qty).putLong(sendNanos);
        w.end();
    }

    public static void cancel(FrameWriter w, long clOrdId, long orderId) {
        w.begin(CANCEL, 16).putLong(clOrdId).putLong(orderId);
        w.end();
    }

    public static void ack(FrameWriter w, long clOrdId, long orderId, long sendNanos) {
        w.begin(ACK, 24).putLong(clOrdId).putLong(orderId).putLong(sendNanos);
        w.end();
    }

    public static void fill(FrameWriter w, long orderId, long price, long qty) {
        w.begin(FILL, 24).putLong(orderId).putLong(price).putLong(qty);
        w.end();
    }

    public static void cancelled(FrameWriter w, long orderId, long leaves) {
        w.begin(CANCELLED, 16).putLong(orderId).putLong(leaves);
        w.end();
    }

    public static void reject(FrameWriter w, long clOrdId, int reason) {
        w.begin(REJECT, 9).putLong(clOrdId).put((byte) reason);
        w.end();
    }

    public static void notLeader(FrameWriter w, int leaderId) {
        w.begin(NOT_LEADER, 4).putInt(leaderId);
        w.end();
    }

    // --- decode: `at` is the index of the type byte ---

    public static byte type(ByteBuffer b, int at) { return b.get(at); }

    public static int logonClientId(ByteBuffer b, int at) { return b.getInt(at + 1); }

    public static long clOrdId(ByteBuffer b, int at) { return b.getLong(at + 1); }
    public static Side side(ByteBuffer b, int at) { return Side.values()[b.get(at + 9)]; }
    public static OrderType orderType(ByteBuffer b, int at) { return OrderType.values()[b.get(at + 10)]; }
    public static long price(ByteBuffer b, int at) { return b.getLong(at + 11); }
    public static long qty(ByteBuffer b, int at) { return b.getLong(at + 19); }
    public static long sendNanos(ByteBuffer b, int at) { return b.getLong(at + 27); }

    public static long cancelOrderId(ByteBuffer b, int at) { return b.getLong(at + 9); }

    public static long ackOrderId(ByteBuffer b, int at) { return b.getLong(at + 9); }
    public static long ackSendNanos(ByteBuffer b, int at) { return b.getLong(at + 17); }

    public static long fillOrderId(ByteBuffer b, int at) { return b.getLong(at + 1); }
    public static long fillPrice(ByteBuffer b, int at) { return b.getLong(at + 9); }
    public static long fillQty(ByteBuffer b, int at) { return b.getLong(at + 17); }

    public static long cancelledOrderId(ByteBuffer b, int at) { return b.getLong(at + 1); }
    public static long cancelledLeaves(ByteBuffer b, int at) { return b.getLong(at + 9); }

    public static int rejectReason(ByteBuffer b, int at) { return b.get(at + 9) & 0xFF; }

    public static int notLeaderId(ByteBuffer b, int at) { return b.getInt(at + 1); }
}
