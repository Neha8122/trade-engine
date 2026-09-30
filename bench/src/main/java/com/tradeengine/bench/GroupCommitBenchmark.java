package com.tradeengine.bench;

import com.tradeengine.exchange.BookFactory;
import com.tradeengine.exchange.ExchangeNode;
import com.tradeengine.exchange.InProcessCluster;
import com.tradeengine.orderbook.OrderBook;
import com.tradeengine.orderbook.OrderType;
import com.tradeengine.orderbook.Side;
import com.tradeengine.raft.FileStorage;
import com.tradeengine.raft.RaftNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Cost per order of getting it committed on a 3-node Raft cluster, for
 * different batch sizes: the group-commit graph.
 *
 * <p>Each invocation pushes 512 orders through the cluster in batches of
 * {@code batch}: the leader proposes a batch (one log append, one fsync),
 * followers store it (one fsync each) and reply, the leader commits and
 * applies it to its order book. Measured until every order is applied on
 * the leader, i.e. could be acknowledged to its client.
 *
 * <p>Caveat: all three nodes run in this one thread with an instant
 * network, so the three fsyncs per batch happen one after another. On three
 * real machines they happen in parallel, but the network adds a round trip.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
public class GroupCommitBenchmark {

    private static final int ORDERS_PER_INVOCATION = 512;
    private static final long MID = 100_100;

    @Param({"1", "8", "64", "512"})
    int batch;

    @Param({"memory", "file"})
    String storage;

    private InProcessCluster cluster;
    private Path dir;
    private long clOrdId;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
        BookFactory books = reports -> new OrderBook(1, 100_000, 200, 10_000, reports);
        InProcessCluster.Disks disks;
        if (storage.equals("file")) {
            dir = Files.createTempDirectory("group-commit-");
            disks = node -> new FileStorage(dir.resolve("node-" + node));
        } else {
            disks = InProcessCluster.inMemory();
        }
        cluster = new InProcessCluster(3, 1, RaftNode.Config.DEFAULT, disks, books,
                node -> new CountingListener(), (client, id) -> { });
        cluster.awaitLeader(5_000);
    }

    @Benchmark
    @OperationsPerInvocation(ORDERS_PER_INVOCATION)
    public long commit512Orders() {
        ExchangeNode leader = cluster.node(cluster.leader());
        for (int sent = 0; sent < ORDERS_PER_INVOCATION; sent += batch) {
            for (int i = 0; i < batch; i += 2) {
                // Rest a sell, then take it with an IOC: the book stays small.
                leader.submitNew(1, ++clOrdId, Side.SELL, OrderType.LIMIT, MID, 1, clOrdId);
                leader.submitNew(1, ++clOrdId, Side.BUY, OrderType.IOC, MID, 1, clOrdId);
            }
            leader.flush();
            long target = leader.lastIndex();
            while (leader.lastApplied() < target) {
                cluster.step();                 // replicate, collect acks, commit, apply
            }
        }
        return leader.lastApplied();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        cluster.close();
        if (dir != null) {
            try (Stream<Path> files = Files.walk(dir)) {
                files.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }
}
