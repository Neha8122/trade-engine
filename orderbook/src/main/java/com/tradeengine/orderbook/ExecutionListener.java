package com.tradeengine.orderbook;

/**
 * Receives everything the book decides. The book itself does no I/O; the
 * listener turns these calls into execution reports and market data.
 *
 * <p>Every argument is a primitive or an enum constant, so reporting an
 * event never creates an object. Calls happen on the book's thread, in the
 * order the book made the decisions, and must not call back into the book.
 */
public interface ExecutionListener {

    /** A new order passed validation and got {@code orderId}. */
    void onAccepted(long orderId, long clOrdId);

    /** {@code qty} traded at {@code price}, always the resting order's price. */
    void onTrade(long makerOrderId, long takerOrderId, long price, long qty);

    /** {@code leaves} of the order were cancelled; it is no longer live. */
    void onCancelled(long orderId, long leaves);

    /** A new order or cancel was refused; nothing in the book changed. */
    void onRejected(long clOrdId, RejectReason reason);
}
