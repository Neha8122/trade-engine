package com.tradeengine.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradeengine.exchange.BookFactory;
import com.tradeengine.orderbook.ExecutionListener;
import com.tradeengine.orderbook.OrderBook;
import com.tradeengine.orderbook.OrderType;
import com.tradeengine.orderbook.RejectReason;
import com.tradeengine.orderbook.Side;
import com.tradeengine.raft.FileStorage;
import com.tradeengine.raft.RaftNode;
import com.tradeengine.raft.Role;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Three real servers on localhost: separate threads, real TCP between them,
 * real files with fsync, real time. No simulator anywhere.
 */
class NodeClusterTest {

    private static final BookFactory BOOKS = r -> new OrderBook(1, 100_000, 200, 10_000, r);

    @TempDir
    Path dir;

    private InetSocketAddress[] addresses;
    private final NodeServer[] servers = new NodeServer[3];
    /** Each server's book events; only touched on that server's loop thread. */
    private final List<List<String>> events = new ArrayList<>();

    @AfterEach
    void stopAll() throws IOException {
        for (NodeServer s : servers) {
            if (s != null) {
                s.close();
            }
        }
    }

    @Test
    void electsOneLeaderAndReplicatesOverTcp() throws Exception {
        startCluster();
        int leader = awaitLeader(-1);

        for (int i = 0; i < 3; i++) {
            if (i != leader) {
                int follower = i;
                awaitTrue(() -> servers[follower].leaderId() == leader, "node " + i + " follows " + leader);
            }
        }

        awaitAllApplied(submit(leader, 1, 300));
        assertSameBooks();
    }

    @Test
    void killingTheLeaderElectsAnotherAndTheOldOneCatchesUp() throws Exception {
        startCluster();
        int first = awaitLeader(-1);
        long firstTerm = servers[first].term();
        awaitAllApplied(submit(first, 1, 100));

        servers[first].close();                 // the leader process dies
        servers[first] = null;
        int second = awaitLeader(first);
        assertNotEquals(first, second);
        assertTrue(servers[second].term() > firstTerm);

        long target = submit(second, 101, 100); // the exchange keeps trading
        startServer(first);                     // old leader comes back from its files
        awaitAllApplied(target);
        assertSameBooks();
        assertEquals(Role.FOLLOWER, servers[first].role());
    }

    // --- helpers ---

    private void startCluster() throws IOException {
        addresses = new InetSocketAddress[3];
        for (int i = 0; i < 3; i++) {
            try (ServerSocket s = new ServerSocket(0)) {
                addresses[i] = new InetSocketAddress(InetAddress.getLoopbackAddress(), s.getLocalPort());
            }
            events.add(null);
        }
        for (int i = 0; i < 3; i++) {
            startServer(i);
        }
    }

    private void startServer(int i) throws IOException {
        List<String> e = new ArrayList<>();
        events.set(i, e);
        servers[i] = new NodeServer(i, addresses, new FileStorage(dir.resolve("node-" + i)),
                RaftNode.Config.DEFAULT, BOOKS, recorder(e), (client, clOrdId) -> { });
        servers[i].start();
    }

    /**
     * Submits orders with ids from..from+n-1 on the leader's own thread and
     * returns the log index of the last one: everyone must reach it.
     */
    private long submit(int leader, long from, int n) throws Exception {
        return servers[leader].call(() -> {
            for (long id = from; id < from + n; id++) {
                boolean sell = id % 2 == 0;
                long price = 100_100 + (sell ? 0 : (id % 3) - 1);
                servers[leader].node.submitNew(7, id, sell ? Side.SELL : Side.BUY,
                        OrderType.LIMIT, price, 1 + id % 50, System.nanoTime());
            }
            servers[leader].node.flush();
            return servers[leader].node.lastIndex();
        }).get(5, TimeUnit.SECONDS);
    }

    private int awaitLeader(int notThis) {
        awaitTrue(() -> leader(notThis) != -1, "a leader other than " + notThis);
        return leader(notThis);
    }

    private int leader(int notThis) {
        int found = -1;
        for (int i = 0; i < 3; i++) {
            if (i != notThis && servers[i] != null && servers[i].role() == Role.LEADER) {
                if (found != -1) {
                    return -1;                  // two claimants: wait for it to settle
                }
                found = i;
            }
        }
        return found;
    }

    private void awaitAllApplied(long target) {
        awaitTrue(() -> {
            for (NodeServer s : servers) {
                if (s == null || s.lastApplied() < target) {
                    return false;
                }
            }
            return true;
        }, "every node applied up to index " + target);
    }

    private void assertSameBooks() throws Exception {
        List<List<String>> copies = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            List<String> mine = events.get(i);
            copies.add(servers[i].call(() -> List.copyOf(mine)).get(5, TimeUnit.SECONDS));
        }
        assertTrue(copies.get(0).size() > 100, "books saw " + copies.get(0).size() + " events");
        assertEquals(copies.get(0), copies.get(1), "node 1 differs from node 0");
        assertEquals(copies.get(0), copies.get(2), "node 2 differs from node 0");
    }

    private static void awaitTrue(BooleanSupplier ok, String what) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (!ok.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for: " + what);
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
    }

    private static ExecutionListener recorder(List<String> out) {
        return new ExecutionListener() {
            @Override public void onAccepted(long orderId, long clOrdId) { out.add("A " + orderId + " " + clOrdId); }
            @Override public void onTrade(long m, long t, long p, long q) { out.add("T " + m + " " + t + " " + q + "@" + p); }
            @Override public void onCancelled(long orderId, long leaves) { out.add("C " + orderId + " " + leaves); }
            @Override public void onRejected(long clOrdId, RejectReason r) { out.add("R " + clOrdId + " " + r); }
        };
    }
}
