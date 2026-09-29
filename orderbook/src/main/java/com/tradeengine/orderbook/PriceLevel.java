package com.tradeengine.orderbook;

/**
 * All resting orders at one price, oldest first. The head is always the
 * next order to match, which is what gives time priority.
 *
 * <p>Invariants, checked by the tests:
 * <ul>
 *   <li>{@code totalQty} equals the sum of {@code leaves} of all orders</li>
 *   <li>{@code count} equals the number of linked orders</li>
 *   <li>{@code head.prev == null} and {@code tail.next == null}</li>
 * </ul>
 */
public final class PriceLevel {

    final long price;
    long totalQty;
    int count;
    Order head;
    Order tail;

    PriceLevel(long price) {
        this.price = price;
    }

    /** Adds to the back of the queue: newest order has lowest priority. O(1). */
    void append(Order order) {
        order.level = this;
        order.prev = tail;
        order.next = null;
        if (tail == null) {
            head = order;
        } else {
            tail.next = order;
        }
        tail = order;
        totalQty += order.leaves;
        count++;
    }

    /**
     * Unlinks an order from anywhere in the queue. O(1) because the order
     * carries its own prev/next links, so there is nothing to search.
     */
    void remove(Order order) {
        if (order.prev == null) {
            head = order.next;
        } else {
            order.prev.next = order.next;
        }
        if (order.next == null) {
            tail = order.prev;
        } else {
            order.next.prev = order.prev;
        }
        totalQty -= order.leaves;
        count--;
        order.prev = null;
        order.next = null;
        order.level = null;
    }

    /** Partial fill of a resting order: it keeps its place in the queue. */
    void reduce(Order order, long filled) {
        order.leaves -= filled;
        totalQty -= filled;
    }

    boolean isEmpty() {
        return count == 0;
    }

    public long price() { return price; }
    public long totalQty() { return totalQty; }
    public int count() { return count; }
    public Order head() { return head; }
}
