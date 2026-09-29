package com.tradeengine.orderbook;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PriceLevelTest {

    private PriceLevel level;
    private Order a, b, c;

    @BeforeEach
    void setUp() {
        level = new PriceLevel(100050);
        a = order(1, 300);
        b = order(2, 100);
        c = order(3, 50);
    }

    @Test
    void appendKeepsArrivalOrder() {
        appendAll(a, b, c);

        assertEquals(List.of(a, b, c), walk());
        assertInvariants(3, 450);
        assertSame(level, b.level);
    }

    @Test
    void removeHead() {
        appendAll(a, b, c);
        level.remove(a);

        assertEquals(List.of(b, c), walk());
        assertInvariants(2, 150);
    }

    @Test
    void removeMiddle() {
        appendAll(a, b, c);
        level.remove(b);

        assertEquals(List.of(a, c), walk());
        assertInvariants(2, 350);
    }

    @Test
    void removeTail() {
        appendAll(a, b, c);
        level.remove(c);

        assertEquals(List.of(a, b), walk());
        assertInvariants(2, 400);
    }

    @Test
    void removeOnlyOrderLeavesLevelEmpty() {
        level.append(a);
        level.remove(a);

        assertTrue(level.isEmpty());
        assertNull(level.head);
        assertNull(level.tail);
        assertEquals(0, level.totalQty);
    }

    @Test
    void removedOrderHasNoLinksLeft() {
        appendAll(a, b, c);
        level.remove(b);

        // A pooled order must not point into the book after it leaves it.
        assertNull(b.prev);
        assertNull(b.next);
        assertNull(b.level);
    }

    @Test
    void partialFillKeepsQueuePosition() {
        appendAll(a, b);
        level.reduce(a, 120);

        assertEquals(180, a.leaves);
        assertSame(a, level.head);
        assertInvariants(2, 280);
    }

    @Test
    void appendAfterRemovingEverything() {
        appendAll(a, b);
        level.remove(a);
        level.remove(b);
        level.append(c);

        assertEquals(List.of(c), walk());
        assertInvariants(1, 50);
    }

    // --- helpers ---

    private static Order order(long id, long leaves) {
        Order o = new Order();
        o.orderId = id;
        o.side = Side.SELL;
        o.type = OrderType.LIMIT;
        o.price = 100050;
        o.qty = leaves;
        o.leaves = leaves;
        return o;
    }

    private void appendAll(Order... orders) {
        for (Order o : orders) {
            level.append(o);
        }
    }

    /** Walks head → tail and checks every back-link on the way. */
    private List<Order> walk() {
        List<Order> out = new ArrayList<>();
        Order prev = null;
        for (Order o = level.head; o != null; o = o.next) {
            assertSame(prev, o.prev, "broken prev link at #" + o.orderId);
            out.add(o);
            prev = o;
        }
        assertSame(prev, level.tail, "tail is not the last order");
        return out;
    }

    private void assertInvariants(int count, long totalQty) {
        assertEquals(count, level.count, "count");
        assertEquals(totalQty, level.totalQty, "totalQty");
        long sum = 0;
        for (Order o = level.head; o != null; o = o.next) {
            sum += o.leaves;
        }
        assertEquals(sum, level.totalQty, "totalQty != sum of leaves");
    }
}
