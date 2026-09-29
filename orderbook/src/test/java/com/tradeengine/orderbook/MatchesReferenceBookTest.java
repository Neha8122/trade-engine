package com.tradeengine.orderbook;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Differential test: the fast book and the slow {@link ReferenceBook} get
 * the same random stream of orders and cancels, and must report exactly
 * the same events, in the same order, after every single step.
 *
 * <p>Hand-written tests only cover the cases someone thought of. This one
 * covers the combinations nobody thought of. Seeds are fixed, so any
 * failure reproduces exactly.
 */
class MatchesReferenceBookTest {

    private static final long BASE = 100_000;
    private static final int LEVELS = 200;
    private static final int MAX_ORDERS = 64;      // small: POOL_FULL happens
    private static final int STEPS = 20_000;
    private static final long MID = BASE + LEVELS / 2;

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 4, 5})
    void fastBookMatchesReference(long seed) {
        RecordingListener fastEvents = new RecordingListener();
        RecordingListener refEvents = new RecordingListener();
        OrderBook fast = new OrderBook(1, BASE, LEVELS, MAX_ORDERS, fastEvents);
        ReferenceBook ref = new ReferenceBook(BASE, LEVELS, MAX_ORDERS, refEvents);

        Random rnd = new Random(seed);
        List<Long> issuedIds = new ArrayList<>();
        Map<String, Integer> seen = new HashMap<>();

        for (int step = 0; step < STEPS; step++) {
            long clOrdId = step + 1;
            String what;

            if (!issuedIds.isEmpty() && rnd.nextInt(4) == 0) {
                // Cancel: usually a real id (live, filled or cancelled), sometimes junk.
                long id = rnd.nextInt(10) == 0
                        ? 1_000_000 + rnd.nextInt(1000)
                        : issuedIds.get(rnd.nextInt(issuedIds.size()));
                what = "cancel #" + id;
                fast.cancel(clOrdId, id);
                ref.cancel(clOrdId, id);
            } else {
                Side side = rnd.nextBoolean() ? Side.BUY : Side.SELL;
                OrderType type = randomType(rnd);
                long price = randomPrice(rnd);
                long qty = rnd.nextInt(50) == 0 ? 0 : 1 + rnd.nextInt(100);
                what = side + " " + type + " " + qty + " @" + price;

                long fastId = fast.newOrder(7, clOrdId, side, type, price, qty, step);
                long refId = ref.newOrder(clOrdId, side, type, price, qty);
                assertEquals(refId, fastId, "order id at step " + step + ": " + what);
                if (fastId != 0) {
                    issuedIds.add(fastId);
                }
            }

            List<String> expected = refEvents.drain();
            List<String> actual = fastEvents.drain();
            assertEquals(expected, actual, "events at step " + step + ": " + what);
            assertEquals(ref.bestBid(), price(fast.bestBid()), "best bid at step " + step);
            assertEquals(ref.bestAsk(), price(fast.bestAsk()), "best ask at step " + step);
            assertEquals(ref.liveOrders(), fast.liveOrders(), "live orders at step " + step);

            for (String e : actual) {
                seen.merge(kindOf(e), 1, Integer::sum);
            }
        }

        // Guard against a vacuous pass: every kind of event must have happened.
        for (String kind : List.of("ACCEPTED", "TRADE", "CANCELLED", "REJECTED BAD_QTY",
                "REJECTED OUT_OF_BAND", "REJECTED POOL_FULL", "REJECTED UNKNOWN_ORDER")) {
            assertTrue(seen.getOrDefault(kind, 0) > 0, "never saw " + kind + ", seen: " + seen);
        }
    }

    private static OrderType randomType(Random rnd) {
        int r = rnd.nextInt(100);
        return r < 70 ? OrderType.LIMIT : r < 85 ? OrderType.IOC : OrderType.MARKET;
    }

    /**
     * Mostly near the middle so orders cross often; 1 in 10 anywhere in the
     * band, so best-price searches cross bitmap words and sides go empty;
     * now and then off the band.
     */
    private static long randomPrice(Random rnd) {
        if (rnd.nextInt(40) == 0) {
            return rnd.nextBoolean() ? BASE - 1 - rnd.nextInt(5) : BASE + LEVELS + rnd.nextInt(5);
        }
        if (rnd.nextInt(10) == 0) {
            return BASE + rnd.nextInt(LEVELS);
        }
        return MID + (long) (rnd.nextGaussian() * 8);
    }

    /** "TRADE maker=#1 ..." → "TRADE"; "REJECTED clOrdId=9 BAD_QTY" → "REJECTED BAD_QTY". */
    private static String kindOf(String event) {
        String[] parts = event.split(" ");
        return parts[0].equals("REJECTED") ? "REJECTED " + parts[2] : parts[0];
    }

    private static Long price(PriceLevel level) {
        return level == null ? null : level.price();
    }
}
