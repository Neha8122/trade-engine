package com.tradeengine.exchange;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradeengine.orderbook.ExecutionListener;
import com.tradeengine.orderbook.OrderBook;
import com.tradeengine.orderbook.OrderType;
import com.tradeengine.orderbook.RejectReason;
import com.tradeengine.orderbook.Side;
import com.tradeengine.raft.FileStorage;
import com.tradeengine.raft.RaftNode;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The whole point of Tier 2: three replicated order books.
 *
 * <ul>
 *   <li>every node's book produces exactly the same events, in the same order</li>
 *   <li>an order acknowledged to a client is never lost, across crashes and
 *       leader changes</li>
 *   <li>a client that resends an unacknowledged order never gets it twice</li>
 * </ul>
 */
class ReplicatedExchangeTest {

    private static final BookFactory BOOKS = reports -> new OrderBook(1, 100_000, 200, 5_000, reports);

    @TempDir
    Path tmp;

    /** Per node, per life: the events its book reported. */
    private final List<List<String>> events = new ArrayList<>();
    private final Map<Long, Integer> acked = new HashMap<>();      // clOrdId → times acked

    private InProcessCluster cluster(int size, long seed, InProcessCluster.Disks disks) {
        return cluster(size, seed, disks, RaftNode.Config.DEFAULT);
    }

    private InProcessCluster cluster(int size, long seed, InProcessCluster.Disks disks, RaftNode.Config config) {
        for (int i = 0; i < size; i++) {
            events.add(new ArrayList<>());
        }
        return new InProcessCluster(size, seed, config, disks, BOOKS,
                node -> {
                    List<String> e = new ArrayList<>();
                    events.set(node, e);        // a restarted node starts a new list
                    return recorder(e);
                },
                (clientId, clOrdId) -> acked.merge(clOrdId, 1, Integer::sum));
    }

    @Test
    void threeBooksStayIdentical() {
        InProcessCluster c = cluster(3, 1, InProcessCluster.inMemory());
        int leader = c.awaitLeader(2_000);
        Random rnd = new Random(1);
        for (long id = 1; id <= 200; id++) {
            submitRandom(c.node(leader), id, rnd, c.now());
            if (id % 20 == 0) {
                c.node(leader).flush();         // batches of 20: group commit
            }
        }
        runUntilConverged(c, 2_000);

        assertEquals(200, acked.size(), "every order acknowledged");
        assertTrue(events.get(0).size() > 200);
        assertEquals(events.get(0), events.get(1));
        assertEquals(events.get(0), events.get(2));
    }

    @Test
    void followersCannotTakeOrders() {
        InProcessCluster c = cluster(3, 2, InProcessCluster.inMemory());
        int leader = c.awaitLeader(2_000);
        ExchangeNode follower = c.node((leader + 1) % 3);
        assertTrue(!follower.submitNew(7, 1, Side.BUY, OrderType.LIMIT, 100_050, 10, 0));
    }

    static LongStream seeds() {
        return LongStream.rangeClosed(1, 30);
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void chaosInMemory(long seed) {
        chaos(cluster(seed % 2 == 0 ? 5 : 3, seed, InProcessCluster.inMemory()), seed, 2_000, true);
    }

    /**
     * With snapshots every 25 entries a restarted node rebuilds its book from
     * a snapshot, not from the start, so its event list only has what came
     * after. What must match across nodes is then the book itself.
     */
    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {41, 42, 43, 44, 45, 46, 47, 48, 49, 50})
    void chaosWithSnapshots(long seed) {
        InProcessCluster c = cluster(3, seed, InProcessCluster.inMemory(),
                RaftNode.Config.DEFAULT.withSnapshotEvery(25));
        chaos(c, seed, 2_000, false);
        String first = bookState(c.node(0).book());
        for (int i = 1; i < c.size; i++) {
            assertEquals(first, bookState(c.node(i).book()), "node " + i + " book differs");
        }
    }

    /** Every resting order with all its fields, in time-priority order. */
    private static String bookState(OrderBook book) {
        StringBuilder s = new StringBuilder("next=" + book.nextOrderId() + " last=" + book.lastTradePrice());
        book.forEachResting(o -> s.append(' ').append(o));
        return s.toString();
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {101, 102, 103})
    void chaosOnRealFiles(long seed) {
        Path dir = tmp.resolve("seed-" + seed);
        chaos(cluster(3, seed, node -> new FileStorage(dir.resolve("node-" + node))), seed, 1_000, true);
    }

    /**
     * Clients keep sending orders to whoever is leader and resend anything
     * not acknowledged after 20 ticks, while nodes crash, restart and get
     * partitioned. At the end, with the network healed, every order must be
     * acknowledged, applied exactly once, and every book identical.
     */
    /** @param fullHistory every node's event list covers the whole run (no snapshots) */
    private void chaos(InProcessCluster c, long seed, int steps, boolean fullHistory) {
        c.dropRate = 0.05;
        c.maxDelay = 4;
        Random chaos = new Random(seed * 17);
        Random orders = new Random(seed * 31);
        Map<Long, Long> pending = new LinkedHashMap<>();   // clOrdId → tick last sent
        Map<Long, Long> seedOf = new HashMap<>();          // clOrdId → random seed of its fields
        long nextId = 1;
        int resends = 0;

        for (int t = 0; t < steps + 20_000; t++) {
            boolean calm = t >= steps;
            if (calm && t == steps) {
                c.heal();
                c.dropRate = 0;
                c.maxDelay = 1;
                for (int i = 0; i < c.size; i++) {
                    c.restart(i);
                }
            }
            if (!calm) {
                int r = chaos.nextInt(1_000);
                if (r < 3) {
                    c.crash(chaos.nextInt(c.size));
                } else if (r < 15) {
                    c.restart(chaos.nextInt(c.size));
                } else if (r < 20) {
                    for (int i = 0; i < c.size; i++) {
                        c.setPartition(i, chaos.nextInt(2));
                    }
                } else if (r < 30) {
                    c.heal();
                }
            }

            int leader = c.leader();
            if (leader != -1) {
                ExchangeNode l = c.node(leader);
                // New orders while the chaos lasts: a few per tick.
                if (!calm && orders.nextInt(3) == 0) {
                    long id = nextId++;
                    seedOf.put(id, orders.nextLong());
                    pending.put(id, -1L);
                }
                // Send new ones, and resend anything unacked for 20 ticks.
                for (Map.Entry<Long, Long> p : pending.entrySet()) {
                    long lastSent = p.getValue();
                    if (lastSent == -1 || c.now() - lastSent >= 20) {
                        if (submitRandom(l, p.getKey(), new Random(seedOf.get(p.getKey())), c.now())) {
                            if (lastSent != -1) {
                                resends++;
                            }
                            p.setValue(c.now());
                        }
                    }
                }
                l.flush();                       // one batch per tick
            }
            c.step();
            pending.keySet().removeIf(acked::containsKey);

            if (calm && pending.isEmpty() && converged(c)) {
                break;
            }
        }
        c.close();

        assertTrue(pending.isEmpty(), pending.size() + " orders never acknowledged");
        assertTrue(converged(c), "nodes did not converge");
        if (!fullHistory) {
            return;                             // snapshots: the caller compares books instead
        }
        for (long id : acked.keySet()) {
            assertTrue(events.get(0).stream().anyMatch(e -> e.endsWith("clOrd=" + id)),
                    "order " + id + " was acknowledged but never reached the book: LOST");
        }
        assertTrue(acked.size() > 100, "too few orders got through: " + acked.size());
        assertTrue(resends > 0, "no resends happened, so dedup went untested");

        for (int i = 1; i < c.size; i++) {
            assertEquals(events.get(0), events.get(i), "node " + i + " book differs from node 0");
        }
        // Each order id reaches the book at most once, however often it was sent.
        Map<String, Integer> outcomes = new HashMap<>();
        for (String e : events.get(0)) {
            if (e.startsWith("ACCEPTED") || e.startsWith("REJECTED")) {
                outcomes.merge(e.substring(e.indexOf("clOrd=")), 1, Integer::sum);
            }
        }
        outcomes.forEach((k, n) -> assertEquals(1, n, k + " applied " + n + " times"));
        assertEquals(nextId - 1, outcomes.size(), "every order applied exactly once");
    }

    /**
     * A leader cut off from the others accepts two orders into its own log.
     * The majority elects a new leader, which puts different entries at those
     * log indexes. When the old leader rejoins, its entries are replaced; it
     * must not acknowledge its clients just because *something* got applied
     * at the index where it proposed.
     */
    @Test
    void overwrittenProposalsAreNotAcknowledged() {
        InProcessCluster c = cluster(3, 7, InProcessCluster.inMemory());
        int old = c.awaitLeader(2_000);
        c.setPartition(old, 1);                 // alone on its side
        c.node(old).submitNew(7, 901, Side.BUY, OrderType.LIMIT, 100_040, 1, 0);
        c.node(old).submitNew(7, 902, Side.BUY, OrderType.LIMIT, 100_040, 1, 0);
        c.node(old).flush();

        for (int i = 0; i < 3_000 && (c.leader() == -1 || c.leader() == old); i++) {
            c.step();
        }
        int fresh = c.leader();
        for (long id = 1; id <= 3; id++) {
            c.node(fresh).submitNew(7, id, Side.SELL, OrderType.LIMIT, 100_060, 1, 0);
        }
        c.node(fresh).flush();
        c.heal();
        runUntilConverged(c, 3_000);

        assertEquals(java.util.Set.of(1L, 2L, 3L), acked.keySet(),
                "only the new leader's orders were committed and acknowledged");
        assertEquals(events.get(fresh), events.get(old));
    }

    // --- helpers ---

    /** Same clOrdId always gets the same fields, however often it's resent. */
    private static boolean submitRandom(ExchangeNode leader, long clOrdId, Random r, long now) {
        Side side = r.nextBoolean() ? Side.BUY : Side.SELL;
        OrderType type = r.nextInt(10) < 8 ? OrderType.LIMIT : OrderType.IOC;
        long price = 100_100 + (long) (r.nextGaussian() * 8);
        return leader.submitNew(7, clOrdId, side, type, price, 1 + r.nextInt(100), now);
    }

    private void runUntilConverged(InProcessCluster c, int maxSteps) {
        for (int i = 0; i < maxSteps && !converged(c); i++) {
            c.step();
        }
        assertTrue(converged(c), "did not converge");
    }

    private static boolean converged(InProcessCluster c) {
        int leader = c.leader();
        if (leader == -1) {
            return false;
        }
        ExchangeNode l = c.node(leader);
        if (l.commitIndex() < l.lastIndex()) {
            return false;
        }
        for (int i = 0; i < c.size; i++) {
            if (!c.isUp(i) || c.node(i).lastApplied() != l.commitIndex()) {
                return false;
            }
        }
        return true;
    }

    private static ExecutionListener recorder(List<String> out) {
        return new ExecutionListener() {
            @Override public void onAccepted(long orderId, long clOrdId) {
                out.add("ACCEPTED #" + orderId + " clOrd=" + clOrdId);
            }
            @Override public void onTrade(long maker, long taker, long price, long qty) {
                out.add("TRADE " + maker + "/" + taker + " " + qty + "@" + price);
            }
            @Override public void onCancelled(long orderId, long leaves) {
                out.add("CANCELLED #" + orderId + " " + leaves);
            }
            @Override public void onRejected(long clOrdId, RejectReason reason) {
                out.add("REJECTED " + reason + " clOrd=" + clOrdId);
            }
        };
    }
}
