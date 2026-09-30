package com.tradeengine.exchange;

import com.tradeengine.orderbook.ExecutionListener;
import com.tradeengine.raft.FileStorage;
import com.tradeengine.raft.InMemoryStorage;
import com.tradeengine.raft.Message;
import com.tradeengine.raft.RaftNode;
import com.tradeengine.raft.Role;
import com.tradeengine.raft.Storage;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.IntFunction;

/**
 * A whole replicated exchange in one thread: N {@link ExchangeNode}s, a fake
 * network and a fake clock, driven by one seed. Used by the chaos tests and
 * by the group-commit benchmark.
 *
 * <p>The network can drop and delay messages and split nodes into
 * partitions. A crashed node is abandoned; a restarted one gets a new, empty
 * book and rebuilds it by re-applying the committed log from its storage.
 */
public final class InProcessCluster {

    private record InFlight(long deliverAt, Message message) { }

    /** Opens node i's storage: the same object for memory, a reload for files. */
    @FunctionalInterface
    public interface Disks {
        Storage open(int node);
    }

    public static Disks inMemory() {
        Map<Integer, Storage> disks = new HashMap<>();
        return node -> disks.computeIfAbsent(node, n -> new InMemoryStorage());
    }

    public final int size;
    private final Random rnd;
    private final RaftNode.Config config;
    private final Disks disks;
    private final BookFactory books;
    private final IntFunction<ExecutionListener> reports;   // per node, per life
    private final ExchangeNode.AckListener acks;
    private final ExchangeNode[] nodes;
    private final Storage[] storages;
    private final int[] partition;
    private final List<InFlight> network = new ArrayList<>();

    public double dropRate;
    public int maxDelay = 1;
    private long now;

    public InProcessCluster(int size, long seed, RaftNode.Config config, Disks disks,
                            BookFactory books, IntFunction<ExecutionListener> reports,
                            ExchangeNode.AckListener acks) {
        this.size = size;
        this.rnd = new Random(seed);
        this.config = config;
        this.disks = disks;
        this.books = books;
        this.reports = reports;
        this.acks = acks;
        this.nodes = new ExchangeNode[size];
        this.storages = new Storage[size];
        this.partition = new int[size];
        for (int i = 0; i < size; i++) {
            start(i);
        }
    }

    /** One tick: deliver due messages, then tick every live node. */
    public void step() {
        now++;
        List<InFlight> due = new ArrayList<>();
        network.removeIf(f -> {
            if (f.deliverAt <= now) {
                due.add(f);
                return true;
            }
            return false;
        });
        for (InFlight f : due) {
            Message m = f.message;
            ExchangeNode to = nodes[m.to()];
            if (to != null && partition[m.from()] == partition[m.to()]) {
                to.handle(m);
            }
        }
        for (ExchangeNode n : nodes) {
            if (n != null) {
                n.tick();
            }
        }
    }

    /** The live leader with the highest term, or -1. */
    public int leader() {
        int best = -1;
        for (int i = 0; i < size; i++) {
            ExchangeNode n = nodes[i];
            if (n != null && n.role() == Role.LEADER && (best == -1 || n.term() > nodes[best].term())) {
                best = i;
            }
        }
        return best;
    }

    public int awaitLeader(int maxSteps) {
        for (int i = 0; i < maxSteps && leader() == -1; i++) {
            step();
        }
        if (leader() == -1) {
            throw new IllegalStateException("no leader after " + maxSteps + " steps");
        }
        return leader();
    }

    public ExchangeNode node(int i) {
        return nodes[i];
    }

    public long now() {
        return now;
    }

    // --- faults ---

    public void crash(int i) {
        nodes[i] = null;
    }

    public void restart(int i) {
        if (nodes[i] == null) {
            start(i);
        }
    }

    public boolean isUp(int i) {
        return nodes[i] != null;
    }

    public void setPartition(int node, int group) {
        partition[node] = group;
    }

    public void heal() {
        Arrays.fill(partition, 0);
    }

    public void close() {
        for (Storage s : storages) {
            closeQuietly(s);
        }
    }

    private void start(int i) {
        closeQuietly(storages[i]);              // the dead process's file handles
        storages[i] = disks.open(i);
        nodes[i] = new ExchangeNode(i, size, storages[i],
                m -> {
                    if (rnd.nextDouble() >= dropRate) {
                        network.add(new InFlight(now + 1 + rnd.nextInt(maxDelay), m));
                    }
                },
                new Random(rnd.nextLong()), config, books, reports.apply(i), acks);
    }

    private static void closeQuietly(Storage s) {
        if (s instanceof FileStorage f) {
            try {
                f.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
