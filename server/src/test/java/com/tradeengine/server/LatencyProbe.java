package com.tradeengine.server;

import com.tradeengine.orderbook.OrderBook;
import com.tradeengine.orderbook.OrderType;
import com.tradeengine.orderbook.Side;
import com.tradeengine.raft.FileStorage;
import com.tradeengine.raft.InMemoryStorage;
import com.tradeengine.raft.RaftNode;
import com.tradeengine.raft.Role;
import com.tradeengine.raft.Storage;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.Arrays;

/** Diagnostic: order -> ack latency of an in-JVM 3-node cluster, with and without fsync. */
public final class LatencyProbe {
    public static void main(String[] args) throws Exception {
        for (String mode : new String[] {"memory", "fsync"}) {
            InetSocketAddress[] peers = new InetSocketAddress[3], clients = new InetSocketAddress[3];
            for (int i = 0; i < 3; i++) { peers[i] = free(); clients[i] = free(); }
            GatewayServer[] s = new GatewayServer[3];
            for (int i = 0; i < 3; i++) {
                Storage st = mode.equals("memory") ? new InMemoryStorage()
                        : FileStorage.groupCommit(Files.createTempDirectory("probe"));
                s[i] = new GatewayServer(i, peers, clients[i], st, RaftNode.Config.DEFAULT,
                        r -> new OrderBook(1, 100_000, 2_000, 100_000, r),
                        new GatewayServer.Limits(10_000, 1_000, 100_000, 1e9, 1_000_000));
                s[i].start();
            }
            while (Arrays.stream(s).noneMatch(x -> x.role() == Role.LEADER && x.commitIndex() > 0)) Thread.sleep(5);
            long[] lat = new long[2000]; int[] n = {0};
            ExchangeClient c = new ExchangeClient(clients, 1, new ExchangeClient.Listener() {
                public void onAck(long id, long o, long l) { if (n[0] < lat.length) lat[n[0]++] = l; }
                public void onReject(long id, int r) { }
            });
            for (long id = 1; id <= lat.length; id++) {       // one at a time: pure round-trip latency
                int before = n[0];
                c.send(id, Side.BUY, OrderType.IOC, 101_000, 1, System.nanoTime());
                while (n[0] == before) c.poll();
            }
            Arrays.sort(lat);
            System.out.printf("%-6s one order at a time: p50 %.2f ms  p99 %.2f ms%n", mode,
                    lat[lat.length / 2] / 1e6, lat[lat.length * 99 / 100] / 1e6);
            c.close();
            for (GatewayServer x : s) x.close();
        }
    }
    private static InetSocketAddress free() throws Exception {
        try (ServerSocket x = new ServerSocket(0)) { return new InetSocketAddress(InetAddress.getLoopbackAddress(), x.getLocalPort()); }
    }
}
