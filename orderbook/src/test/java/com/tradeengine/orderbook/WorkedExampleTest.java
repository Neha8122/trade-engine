package com.tradeengine.orderbook;

import static com.tradeengine.orderbook.OrderType.LIMIT;
import static com.tradeengine.orderbook.Side.BUY;
import static com.tradeengine.orderbook.Side.SELL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * docs/lld-orderbook.html section 4, exactly: #20 BUY LIMIT 700 @ 1000.51
 * sweeps 1000.50 and part of 1000.51.
 *
 * <p>The book assigns its own ids, so orders are sent in the doc's id
 * order (which is also arrival order) and doc ids are mapped to real ids.
 */
class WorkedExampleTest {

    private final RecordingListener events = new RecordingListener();
    private final OrderBook book = new OrderBook(1, 100_000, 200, 32, events);
    private final Map<Integer, Long> id = new HashMap<>();

    @Test
    void buySweepsTwoLevels() {
        // Book before
        place(3, BUY, 100047, 250);
        place(7, BUY, 100048, 400);
        place(9, SELL, 100053, 200);
        place(11, SELL, 100050, 300);
        place(12, SELL, 100051, 500);
        place(14, SELL, 100050, 100);
        place(15, SELL, 100053, 50);
        events.drain();

        place(20, BUY, 100051, 700);

        // Loop, step by step: three fills, all at the makers' prices
        assertEquals(List.of(
                "ACCEPTED #" + id.get(20),
                "TRADE maker=#" + id.get(11) + " taker=#" + id.get(20) + " 300 @100050",
                "TRADE maker=#" + id.get(14) + " taker=#" + id.get(20) + " 100 @100050",
                "TRADE maker=#" + id.get(12) + " taker=#" + id.get(20) + " 300 @100051"),
                events.drain());

        // Book after
        assertTrue(book.level(SELL, 100050).isEmpty());
        assertEquals(100051, book.bestAsk().price());
        assertEquals(orders(12), queue(SELL, 100051));
        assertEquals(200, book.order(id.get(12)).leaves());
        assertEquals(orders(9, 15), queue(SELL, 100053));
        assertEquals(250, book.level(SELL, 100053).totalQty());

        assertEquals(100048, book.bestBid().price());
        assertEquals(orders(7), queue(BUY, 100048));
        assertEquals(orders(3), queue(BUY, 100047));

        assertNull(book.order(id.get(20)));     // fully filled, never rested
        assertEquals(5, book.liveOrders());     // 3, 7, 9, 12, 15
    }

    private void place(int docId, Side side, long price, long qty) {
        id.put(docId, book.newOrder(1, docId, side, LIMIT, price, qty, docId));
    }

    private List<Long> orders(int... docIds) {
        List<Long> out = new ArrayList<>();
        for (int d : docIds) {
            out.add(id.get(d));
        }
        return out;
    }

    private List<Long> queue(Side side, long price) {
        List<Long> out = new ArrayList<>();
        for (Order o = book.level(side, price).head(); o != null; o = o.next) {
            out.add(o.orderId());
        }
        return out;
    }
}
