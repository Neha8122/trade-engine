package com.tradeengine.server;

import com.tradeengine.exchange.BookFactory;
import com.tradeengine.orderbook.OrderBook;
import com.tradeengine.raft.FileStorage;
import com.tradeengine.raft.RaftNode;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Entry point of {@code trade-engine.jar}.
 *
 * <pre>
 * java -jar trade-engine.jar node --id 0 --peers h:p,h:p,h:p --clients h:p,h:p,h:p --data dir
 * java -jar trade-engine.jar loadgen --clients h:p,h:p,h:p --rate 10000 --seconds 20 [--warmup 5]
 * </pre>
 */
public final class Main {

    private Main() { }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            return;
        }
        Map<String, String> opts = options(Arrays.copyOfRange(args, 1, args.length));
        switch (args[0]) {
            case "node" -> node(opts);
            case "loadgen" -> LoadGenerator.run(opts);
            default -> usage();
        }
    }

    private static void node(Map<String, String> o) throws Exception {
        int id = Integer.parseInt(o.get("id"));
        InetSocketAddress[] peers = addresses(o.get("peers"));
        InetSocketAddress[] clients = addresses(o.get("clients"));
        BookFactory books = r -> new OrderBook(1, 100_000, 2_000, 1_000_000, r);
        GatewayServer server = new GatewayServer(id, peers, clients[id],
                // Blocking fsync per turn measured faster than the fsync thread on
                // one shared disk (README); --async-fsync true to try the other.
                "true".equals(o.get("async-fsync"))
                        ? FileStorage.asyncGroupCommit(Path.of(o.get("data")))
                        : FileStorage.groupCommit(Path.of(o.get("data"))),
                RaftNode.Config.DEFAULT.withSnapshotEvery(100_000), books,
                new GatewayServer.Limits(10_000, 1_000, 1_000_000, 1e9, 1_000_000));
        server.start();
        System.out.printf("node %d: peers on %s, clients on %s%n", id, peers[id], clients[id]);
        // One status line a second: the run scripts read these to find the leader.
        while (true) {
            Thread.sleep(1_000);
            System.out.printf("node %d role=%s term=%d commit=%d applied=%d%n", id, server.role(),
                    server.term(), server.commitIndex(), server.lastApplied());
        }
    }

    static InetSocketAddress[] addresses(String csv) {
        return Arrays.stream(csv.split(","))
                .map(hp -> {
                    int colon = hp.lastIndexOf(':');
                    return new InetSocketAddress(hp.substring(0, colon), Integer.parseInt(hp.substring(colon + 1)));
                })
                .toArray(InetSocketAddress[]::new);
    }

    private static Map<String, String> options(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            m.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        return m;
    }

    private static void usage() {
        System.out.println("""
                usage:
                  node    --id N --peers h:p,h:p,h:p --clients h:p,h:p,h:p --data DIR [--async-fsync true]
                  loadgen --clients h:p,h:p,h:p --rate ORDERS_PER_SEC --seconds S [--warmup S]""");
    }
}
