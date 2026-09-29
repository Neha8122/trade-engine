package com.tradeengine.orderbook;

/**
 * Fixed set of {@link Order} objects, all created at startup and reused.
 * After construction nothing here allocates, so the book never produces
 * garbage and the GC never has a reason to pause matching.
 *
 * <p>A plain array used as a stack: acquire pops, release pushes. The most
 * recently released order is handed out next, which is also the one most
 * likely to still be in CPU cache.
 */
public final class OrderPool {

    private final Order[] free;
    private int top;    // number of free orders; free[top - 1] is next out

    public OrderPool(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        free = new Order[capacity];
        for (int i = 0; i < capacity; i++) {
            Order o = new Order();
            o.pooled = true;
            free[i] = o;
        }
        top = capacity;
    }

    /** Returns a clean order, or {@code null} when the pool is exhausted. */
    Order acquire() {
        if (top == 0) {
            return null;    // caller rejects with POOL_FULL, never allocates
        }
        Order o = free[--top];
        free[top] = null;
        o.pooled = false;
        return o;
    }

    /** Gives an order back. It is wiped so no old state can leak out. */
    void release(Order o) {
        if (o.pooled) {
            // Releasing twice would hand the same object to two owners.
            throw new IllegalStateException("order released twice: " + o);
        }
        o.reset();
        o.pooled = true;
        free[top++] = o;
    }

    public int capacity() { return free.length; }
    public int available() { return top; }
}
