package com.tradeengine.orderbook;

/**
 * A live order. Instances are created once by {@link OrderPool} and reused,
 * so this class is mutable on purpose and has no constructor arguments.
 *
 * <p>Prices are whole ticks in a {@code long} (1000.50 with tick 0.01 is
 * 100050), never {@code double}: prices must compare exactly.
 */
public final class Order {

    long orderId;
    int clientId;
    long clOrdId;
    Side side;          // enum constants are singletons: no allocation
    OrderType type;
    long price;         // in ticks
    long qty;           // original quantity
    long leaves;        // quantity still open
    long timestamp;     // stamped by the sequencer, never read from a clock here

    // Intrusive doubly linked list inside one PriceLevel. Keeping the links
    // in the order itself means the book never allocates list nodes, and
    // cancel can unlink in O(1) without searching.
    Order prev;
    Order next;
    PriceLevel level;   // back-pointer: cancel finds its level directly

    boolean pooled;     // owned by OrderPool; catches double release

    /** Clears every field so a pooled instance can't leak old state. */
    void reset() {
        orderId = 0;
        clientId = 0;
        clOrdId = 0;
        side = null;
        type = null;
        price = 0;
        qty = 0;
        leaves = 0;
        timestamp = 0;
        prev = null;
        next = null;
        level = null;
    }

    public long orderId() { return orderId; }
    public int clientId() { return clientId; }
    public long clOrdId() { return clOrdId; }
    public Side side() { return side; }
    public OrderType type() { return type; }
    public long price() { return price; }
    public long qty() { return qty; }
    public long leaves() { return leaves; }
    public long timestamp() { return timestamp; }

    @Override
    public String toString() {
        return "Order{#" + orderId + ' ' + side + ' ' + type + ' '
                + leaves + '/' + qty + " @" + price + '}';
    }
}
