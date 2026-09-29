package com.tradeengine.orderbook;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class OrderPoolTest {

    @Test
    void handsOutDistinctOrdersUntilEmpty() {
        OrderPool pool = new OrderPool(3);
        Set<Order> seen = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            Order o = pool.acquire();
            assertNotNull(o);
            seen.add(o);
        }
        assertEquals(3, seen.size());
        assertEquals(0, pool.available());
    }

    @Test
    void returnsNullWhenExhaustedInsteadOfAllocating() {
        OrderPool pool = new OrderPool(1);
        pool.acquire();
        assertNull(pool.acquire());
    }

    @Test
    void releasedOrderIsReusedAndWiped() {
        OrderPool pool = new OrderPool(1);
        Order o = pool.acquire();
        o.orderId = 42;
        o.leaves = 500;
        o.side = Side.BUY;
        o.level = new PriceLevel(1);
        pool.release(o);

        Order again = pool.acquire();
        assertSame(o, again);   // same object: no new allocation
        assertEquals(0, again.orderId);
        assertEquals(0, again.leaves);
        assertNull(again.side);
        assertNull(again.level);
    }

    @Test
    void lastReleasedIsFirstOut() {
        OrderPool pool = new OrderPool(2);
        Order a = pool.acquire();
        Order b = pool.acquire();
        pool.release(a);
        pool.release(b);
        assertSame(b, pool.acquire());
        assertSame(a, pool.acquire());
        assertNotSame(a, b);
    }

    @Test
    void doubleReleaseFailsLoudly() {
        OrderPool pool = new OrderPool(2);
        Order o = pool.acquire();
        pool.release(o);
        assertThrows(IllegalStateException.class, () -> pool.release(o));
        assertEquals(2, pool.available());
    }

    @Test
    void rejectsNonPositiveCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new OrderPool(0));
    }
}
