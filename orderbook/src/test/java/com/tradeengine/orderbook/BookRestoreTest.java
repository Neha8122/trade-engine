package com.tradeengine.orderbook;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A restored book must behave exactly like the original from then on:
 * same fills, same order of fills (time priority kept), same order ids.
 */
class BookRestoreTest {

    private static final long BASE = 100_000;
    private static final int LEVELS = 200;

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 4, 5})
    void restoredBookBehavesExactlyLikeTheOriginal(long seed) {
        Random rnd = new Random(seed);
        OrderBook original = new OrderBook(1, BASE, LEVELS, 5_000, new RecordingListener());
        for (int i = 0; i < 5_000; i++) {
            randomCommand(original, rnd, i);
        }

        // Snapshot → restore into a brand-new book.
        List<String> publicEvents = new ArrayList<>();
        OrderBook copy = new OrderBook(1, BASE, LEVELS, 5_000, new RecordingListener(), new BookListener() {
            @Override public void onAdd(long id, Side s, long p, long q) { publicEvents.add("add " + id); }
            @Override public void onExecute(long id, long q, long p) { publicEvents.add("exec " + id); }
            @Override public void onDelete(long id) { publicEvents.add("del " + id); }
        });
        original.forEachResting(o -> copy.restoreResting(o.orderId, o.clientId, o.clOrdId, o.side, o.type,
                o.price, o.qty, o.leaves, o.timestamp));
        copy.restoreCounters(original.nextOrderId(), original.lastTradePrice());

        assertTrue(original.liveOrders() > 50, "book too empty to test anything");
        assertEquals(original.liveOrders(), copy.liveOrders());
        assertEquals(List.of(), publicEvents, "restoring must not publish market data");

        // Now the same future for both: every event must match.
        RecordingListener a = new RecordingListener();
        RecordingListener b = new RecordingListener();
        OrderBook left = rewire(original, a);
        OrderBook right = rewire(copy, b);
        Random future = new Random(seed * 101);
        long futureSeed = future.nextLong();
        Random r1 = new Random(futureSeed);
        Random r2 = new Random(futureSeed);
        for (int i = 0; i < 20_000; i++) {
            randomCommand(left, r1, 10_000 + i);
            randomCommand(right, r2, 10_000 + i);
            assertEquals(a.drain(), b.drain(), "step " + i);
        }
        assertEquals(original.lastTradePrice(), copy.lastTradePrice());
    }

    /** Copies a book into one reporting to {@code listener}, via the same snapshot methods. */
    private static OrderBook rewire(OrderBook from, RecordingListener listener) {
        OrderBook to = new OrderBook(1, BASE, LEVELS, 5_000, listener);
        from.forEachResting(o -> to.restoreResting(o.orderId, o.clientId, o.clOrdId, o.side, o.type,
                o.price, o.qty, o.leaves, o.timestamp));
        to.restoreCounters(from.nextOrderId(), from.lastTradePrice());
        return to;
    }

    private static void randomCommand(OrderBook book, Random rnd, int step) {
        if (book.nextOrderId() > 1 && rnd.nextInt(4) == 0) {
            book.cancel(step, 1 + rnd.nextInt((int) book.nextOrderId() - 1));
            return;
        }
        Side side = rnd.nextBoolean() ? Side.BUY : Side.SELL;
        int t = rnd.nextInt(10);
        OrderType type = t < 7 ? OrderType.LIMIT : t < 9 ? OrderType.IOC : OrderType.MARKET;
        long price = BASE + LEVELS / 2 + (long) (rnd.nextGaussian() * 10);
        book.newOrder(1 + rnd.nextInt(5), step, side, type, price, 1 + rnd.nextInt(100), step);
    }
}
