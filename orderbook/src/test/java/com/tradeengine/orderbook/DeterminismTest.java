package com.tradeengine.orderbook;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Same input ⇒ same output, byte for byte. Raft replication depends on
 * this: every replica replays the same command log and must end up with
 * the same book and emit the same trades (docs/DESIGN.md, D5).
 */
class DeterminismTest {

    @Test
    void sameCommandsGiveSameEvents() {
        List<String> first = run(42);
        List<String> second = run(42);

        assertTrue(first.size() > 10_000, "run too small to mean anything");
        assertEquals(first, second);
    }

    @Test
    void differentCommandsGiveDifferentEvents() {
        // Sanity check that the comparison above can actually fail.
        assertTrue(!run(42).equals(run(43)));
    }

    /** Runs a fixed random command sequence on a fresh book, returns all events. */
    private static List<String> run(long seed) {
        RecordingListener events = new RecordingListener();
        OrderBook book = new OrderBook(1, 100_000, 200, 1_000, events);
        Random rnd = new Random(seed);
        long lastId = 0;

        for (int step = 1; step <= 20_000; step++) {
            if (lastId > 0 && rnd.nextInt(5) == 0) {
                book.cancel(step, 1 + rnd.nextInt((int) lastId));
            } else {
                Side side = rnd.nextBoolean() ? Side.BUY : Side.SELL;
                OrderType type = rnd.nextInt(10) < 8 ? OrderType.LIMIT : OrderType.IOC;
                long price = 100_100 + (long) (rnd.nextGaussian() * 8);
                long id = book.newOrder(7, step, side, type, price, 1 + rnd.nextInt(100), step);
                if (id != 0) {
                    lastId = id;
                }
            }
        }
        return events.drain();
    }
}
