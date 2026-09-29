package com.tradeengine.feed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradeengine.orderbook.ExecutionListener;
import com.tradeengine.orderbook.OrderBook;
import com.tradeengine.orderbook.OrderType;
import com.tradeengine.orderbook.PriceLevel;
import com.tradeengine.orderbook.RejectReason;
import com.tradeengine.orderbook.Side;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Random;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * End to end: real order book → publisher → a fake network that drops,
 * duplicates and reorders packets → feed handler → replica. After tens of
 * thousands of random orders the replica must equal the real book at every
 * price level. With recovery switched off it must not, which proves
 * recovery is what makes it correct.
 */
class LossyNetworkTest {

    private static final long BASE = 100_000;
    private static final int LEVELS = 200;

    /** Drops ~5%, duplicates ~3%, delays ~5% of packets by a few places. */
    static final class LossyNetwork implements PacketSink {
        private final Random rnd;
        private final FeedHandler receiver;
        private final ArrayDeque<ByteBuffer> delayed = new ArrayDeque<>();
        int sent, dropped, duplicated, reordered;

        LossyNetwork(long seed, FeedHandler receiver) {
            this.rnd = new Random(seed);
            this.receiver = receiver;
        }

        @Override
        public void send(ByteBuffer packet) {
            sent++;
            ByteBuffer copy = CapturingSink.copy(packet);
            int r = rnd.nextInt(100);
            if (r < 5) {
                dropped++;
            } else if (r < 10) {
                reordered++;
                delayed.addLast(copy);          // arrives after later packets
            } else {
                deliver(copy);
                if (r < 13) {
                    duplicated++;
                    deliver(CapturingSink.copy(copy));
                }
            }
            if (delayed.size() > 3 || (!delayed.isEmpty() && rnd.nextInt(4) == 0)) {
                deliver(delayed.pollFirst());
            }
        }

        void drainDelayed() {
            while (!delayed.isEmpty()) {
                deliver(delayed.pollFirst());
            }
        }

        private void deliver(ByteBuffer p) {
            receiver.onPacket(p.duplicate().order(p.order()));
        }
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3})
    void replicaMatchesBookDespiteLoss(long seed) {
        BookReplica replica = new BookReplica();
        RetransmitStore store = new RetransmitStore(1 << 16);
        FeedHandler handler = new FeedHandler(replica, store);
        LossyNetwork network = new LossyNetwork(seed, handler);
        OrderBook book = run(seed, network, store);
        network.drainDelayed();

        assertTrue(network.dropped > 50 && network.reordered > 50 && network.duplicated > 20,
                "network too kind: " + network.dropped + "/" + network.reordered + "/" + network.duplicated);
        assertTrue(handler.gaps() > 0 && handler.recovered() > 0);
        assertEquals(0, handler.lost());
        assertSameBook(book, replica);
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3})
    void withoutRecoveryTheReplicaIsWrong(long seed) {
        BookReplica replica = new BookReplica();
        FeedHandler handler = new FeedHandler(replica, null);
        LossyNetwork network = new LossyNetwork(seed, handler);
        OrderBook book = run(seed, network, null);
        network.drainDelayed();

        assertTrue(handler.lost() > 0);
        assertTrue(differs(book, replica), "replica matched even though messages were lost");
    }

    /** Random orders and cancels; the engine flushes after every 1–8 commands. */
    private static OrderBook run(long seed, PacketSink network, RetransmitStore store) {
        MarketDataPublisher publisher = new MarketDataPublisher(
                new Packetizer(Packetizer.DEFAULT_MAX_PACKET, network, store));
        OrderBook book = new OrderBook(1, BASE, LEVELS, 5_000, NO_REPORTS, publisher);
        Random rnd = new Random(seed * 7919);
        long lastId = 0;
        int untilFlush = 1;

        for (int step = 1; step <= 30_000; step++) {
            if (lastId > 0 && rnd.nextInt(4) == 0) {
                book.cancel(step, 1 + rnd.nextInt((int) lastId));
            } else {
                Side side = rnd.nextBoolean() ? Side.BUY : Side.SELL;
                int t = rnd.nextInt(10);
                OrderType type = t < 7 ? OrderType.LIMIT : t < 9 ? OrderType.IOC : OrderType.MARKET;
                long price = BASE + LEVELS / 2 + (long) (rnd.nextGaussian() * 8);
                long id = book.newOrder(7, step, side, type, price, 1 + rnd.nextInt(100), step);
                if (id != 0) {
                    lastId = id;
                }
            }
            if (--untilFlush == 0) {
                publisher.flush();
                untilFlush = 1 + rnd.nextInt(8);
            }
        }
        publisher.flush();
        return book;
    }

    private static void assertSameBook(OrderBook book, BookReplica replica) {
        for (long price = BASE; price < BASE + LEVELS; price++) {
            for (Side side : Side.values()) {
                assertEquals(book.level(side, price).totalQty(), replica.levelQty(side, price),
                        side + " qty at " + price);
            }
        }
        assertEquals(book.liveOrders(), replica.orderCount(), "resting orders");
        assertEquals(price(book.bestBid()), replica.bestBid(), "best bid");
        assertEquals(price(book.bestAsk()), replica.bestAsk(), "best ask");
    }

    private static boolean differs(OrderBook book, BookReplica replica) {
        if (book.liveOrders() != replica.orderCount()) {
            return true;
        }
        for (long price = BASE; price < BASE + LEVELS; price++) {
            for (Side side : Side.values()) {
                if (book.level(side, price).totalQty() != replica.levelQty(side, price)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static Long price(PriceLevel level) {
        return level == null ? null : level.price();
    }

    private static final ExecutionListener NO_REPORTS = new ExecutionListener() {
        @Override public void onAccepted(long orderId, long clOrdId) { }
        @Override public void onTrade(long maker, long taker, long price, long qty) { }
        @Override public void onCancelled(long orderId, long leaves) { }
        @Override public void onRejected(long clOrdId, RejectReason reason) { }
    };
}
