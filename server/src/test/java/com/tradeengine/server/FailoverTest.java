package com.tradeengine.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradeengine.exchange.BookFactory;
import com.tradeengine.orderbook.OrderBook;
import com.tradeengine.orderbook.OrderType;
import com.tradeengine.orderbook.Side;
import com.tradeengine.raft.FileStorage;
import com.tradeengine.raft.RaftNode;
import com.tradeengine.raft.Role;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Kill the leader server while a client is streaming orders over TCP.
 *
 * <p>Every order is a BUY of 1 at the same price, so none of them trade:
 * each one rests. After the run, the resting quantity at that price counts
 * exactly how many orders reached the book. Lost orders make it smaller,
 * duplicated ones make it bigger. It must equal the number sent, on every
 * node.
 */
class FailoverTest {

    private static final int ORDERS = 3_000;
    private static final long PRICE = 100_050;
    // Room for twice the orders, so duplicates would show up as extra
    // quantity in the book rather than as pool-full rejects.
    private static final BookFactory BOOKS = r -> new OrderBook(1, 100_000, 200, 2 * ORDERS, r);
    private static final GatewayServer.Limits LIMITS =
            new GatewayServer.Limits(1_000, 1_000, 2 * ORDERS, 1e9, 1_000_000);

    @TempDir
    Path dir;

    private final GatewayServer[] servers = new GatewayServer[3];
    private InetSocketAddress[] peers;
    private InetSocketAddress[] clientAddr;

    @AfterEach
    void stop() throws IOException {
        for (GatewayServer s : servers) {
            if (s != null) {
                s.close();
            }
        }
    }

    @Test
    void killingTheLeaderMidStreamLosesAndDuplicatesNothing() throws Exception {
        peers = new InetSocketAddress[3];
        clientAddr = new InetSocketAddress[3];
        for (int i = 0; i < 3; i++) {
            peers[i] = freePort();
            clientAddr[i] = freePort();
        }
        for (int i = 0; i < 3; i++) {
            start(i);
        }
        int leader = awaitLeader(-1);

        Set<Long> acked = new HashSet<>();
        int[] connections = new int[1];
        ExchangeClient client = new ExchangeClient(clientAddr, 7, new ExchangeClient.Listener() {
            @Override public void onAck(long clOrdId, long orderId, long latency) { acked.add(clOrdId); }
            @Override public void onReject(long clOrdId, int reason) {
                throw new AssertionError("order " + clOrdId + " rejected: " + reason);
            }
            @Override public void onConnected(int node) { connections[0]++; }
        });

        long killedAt = 0;
        int killed = -1;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        long next = 1;
        while (acked.size() < ORDERS) {
            if (next <= ORDERS) {
                client.send(next, Side.BUY, OrderType.LIMIT, PRICE, 1, System.nanoTime());
                next++;
            }
            client.poll();
            if (killed == -1 && acked.size() >= ORDERS / 3) {
                killed = leader;
                servers[killed].close();        // leader process dies mid-stream
                servers[killed] = null;
                killedAt = System.nanoTime();
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("stuck: " + acked.size() + " acked, " + client.unacked() + " unacked");
            }
        }
        long outageEnd = System.nanoTime();
        client.close();

        // The old leader comes back from its files and catches up.
        start(killed);
        int newLeader = awaitLeader(-1);
        for (int i = 0; i < 3; i++) {
            int n = i;
            awaitTrue(() -> servers[n].lastApplied() >= servers[newLeader].commitIndex()
                    && servers[newLeader].commitIndex() > 0, "node " + i + " caught up");
        }

        for (int i = 0; i < 3; i++) {
            GatewayServer s = servers[i];
            long qty = s.call(() -> s.node.book().level(Side.BUY, PRICE).totalQty()).get(5, TimeUnit.SECONDS);
            assertEquals(ORDERS, qty, "node " + i + ": orders in the book (fewer = lost, more = duplicated)");
        }
        assertTrue(connections[0] >= 2, "client should have reconnected after the kill");
        GatewayServer nl = servers[newLeader];
        long dups = nl.call(() -> nl.node.duplicatesDropped()).get(5, TimeUnit.SECONDS);
        System.out.printf("failover: %d orders, client connected %d times, %.0f ms from kill to all acked,"
                + " %d resends deduplicated%n", ORDERS, connections[0], (outageEnd - killedAt) / 1e6, dups);
    }

    // --- helpers ---

    private void start(int i) throws IOException {
        servers[i] = new GatewayServer(i, peers, clientAddr[i], FileStorage.groupCommit(dir.resolve("n" + i)),
                RaftNode.Config.DEFAULT, BOOKS, LIMITS);
        servers[i].start();
    }

    private int awaitLeader(int notThis) {
        int[] found = new int[1];
        awaitTrue(() -> {
            found[0] = -1;
            for (int i = 0; i < 3; i++) {
                if (i != notThis && servers[i] != null && servers[i].role() == Role.LEADER
                        && servers[i].commitIndex() > 0) {
                    found[0] = i;
                }
            }
            return found[0] != -1;
        }, "a leader");
        return found[0];
    }

    private static void awaitTrue(java.util.function.BooleanSupplier ok, String what) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (!ok.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
    }

    private static InetSocketAddress freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return new InetSocketAddress(InetAddress.getLoopbackAddress(), s.getLocalPort());
        }
    }
}
