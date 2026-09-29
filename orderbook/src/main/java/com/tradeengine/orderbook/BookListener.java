package com.tradeengine.orderbook;

/**
 * Public view of the book: what market data subscribers see. Unlike
 * {@link ExecutionListener} (private, for the order's owner), these events
 * carry no client ids and only describe resting orders:
 *
 * <ul>
 *   <li>an order starts resting → {@link #onAdd}</li>
 *   <li>a resting order is hit → {@link #onExecute}; if its qty reaches 0
 *       it is gone, no separate delete follows</li>
 *   <li>a resting order is cancelled → {@link #onDelete}</li>
 * </ul>
 *
 * An IOC or market order that never rests produces no public event of its
 * own; subscribers only see the executions against the orders it hit.
 */
public interface BookListener {

    void onAdd(long orderId, Side side, long price, long qty);

    /** {@code qty} of resting order {@code orderId} traded at {@code price}. */
    void onExecute(long orderId, long qty, long price);

    void onDelete(long orderId);

    /** For books with no market data feed attached. */
    BookListener NONE = new BookListener() {
        @Override public void onAdd(long orderId, Side side, long price, long qty) { }
        @Override public void onExecute(long orderId, long qty, long price) { }
        @Override public void onDelete(long orderId) { }
    };
}
