package com.tradeengine.raft;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BooleanSupplier;

/**
 * A whole Raft cluster in one thread: nodes, a fake network and a fake
 * clock, all driven by one seeded Random, so any run replays exactly.
 *
 * <p>The network can drop, delay (and so reorder) messages, and split the
 * nodes into partitions. Nodes can crash and restart; their Storage
 * survives, like a disk would.
 *
 * <p>Raft's safety properties are checked while it runs; a violation throws
 * immediately, at the step where it happened.
 */
final class SimCluster {

    private record InFlight(long deliverAt, Message message) { }

    /** Opens node i's storage: the same object for memory, a fresh reload from disk for files. */
    @FunctionalInterface
    interface Disks {
        Storage open(int node);
    }

    /** In-memory disks: each node keeps one storage object for its whole life. */
    static Disks inMemory() {
        java.util.Map<Integer, Storage> disks = new java.util.HashMap<>();
        return node -> disks.computeIfAbsent(node, n -> new InMemoryStorage());
    }

    final int size;
    private final Random rnd;
    private final RaftNode.Config config;
    private final Disks disks;
    private final Storage[] storages;
    private final RaftNode[] nodes;
    private final int[] partition;              // nodes talk only within the same group
    private final List<InFlight> network = new ArrayList<>();
    private final long[] lastCommit;            // per node life: commitIndex never goes down

    /** What each node applied in its current life (reset on restart). */
    final List<List<Long>> applied = new ArrayList<>();
    /** Every command ever applied anywhere, by index: must never disagree. */
    private final Map<Long, Long> appliedAnywhere = new HashMap<>();
    /** Term of the node that first applied each index: when it was known committed. */
    private final Map<Long, Long> committedInTerm = new HashMap<>();
    private final Map<Long, Integer> leaderOfTerm = new HashMap<>();

    double dropRate;
    int maxDelay = 1;
    long now;
    private long nextCommand = 1;

    SimCluster(int size, long seed, RaftNode.Config config, Disks disks) {
        this.size = size;
        this.rnd = new Random(seed);
        this.config = config;
        this.disks = disks;
        this.storages = new Storage[size];
        this.nodes = new RaftNode[size];
        this.partition = new int[size];
        this.lastCommit = new long[size];
        for (int i = 0; i < size; i++) {
            applied.add(new ArrayList<>());
            start(i);
        }
    }

    SimCluster(int size, long seed, RaftNode.Config config) {
        this(size, seed, config, inMemory());
    }

    SimCluster(int size, long seed) {
        this(size, seed, RaftNode.Config.DEFAULT, inMemory());
    }

    // --- driving time ---

    /** One tick: deliver due messages, tick every live node, check safety. */
    void step() {
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
            RaftNode to = nodes[m.to()];
            if (to != null && partition[m.from()] == partition[m.to()]) {
                to.handle(m);
            }
        }
        for (RaftNode n : nodes) {
            if (n != null) {
                n.tick();
            }
        }
        checkElectionSafety();
        checkCommitNeverDecreases();
        if (now % 25 == 0) {
            checkLogMatching();
            checkLeaderCompleteness();
        }
    }

    void run(int steps) {
        for (int i = 0; i < steps; i++) {
            step();
        }
    }

    /** Steps until {@code done} is true; fails if it takes too long. */
    void runUntil(BooleanSupplier done, int maxSteps, String what) {
        for (int i = 0; i < maxSteps; i++) {
            if (done.getAsBoolean()) {
                return;
            }
            step();
        }
        if (!done.getAsBoolean()) {
            throw new AssertionError("gave up waiting for: " + what + " (t=" + now + ")");
        }
    }

    // --- faults ---

    void crash(int i) {
        nodes[i] = null;
    }

    void restart(int i) {
        if (nodes[i] == null) {
            applied.get(i).clear();             // new life: state machine starts empty
            lastCommit[i] = 0;
            start(i);
        }
    }

    boolean isUp(int i) {
        return nodes[i] != null;
    }

    /** Puts each listed node in its own group, cut off from everyone else. */
    void isolate(int... ids) {
        for (int i : ids) {
            partition[i] = 100 + i;
        }
    }

    void setPartition(int node, int group) {
        partition[node] = group;
    }

    void heal() {
        Arrays.fill(partition, 0);
    }

    // --- clients ---

    /** Proposes a new unique command to node i; returns its log index or -1. */
    long propose(int i) {
        return propose(i, nextCommand++);
    }

    long propose(int i, long command) {
        RaftNode n = nodes[i];
        return n == null ? -1 : n.propose(ByteBuffer.allocate(8).putLong(command).array());
    }

    /** The live leader with the highest term, or -1. */
    int leader() {
        int best = -1;
        for (int i = 0; i < size; i++) {
            RaftNode n = nodes[i];
            if (n != null && n.role() == Role.LEADER && (best == -1 || n.term() > nodes[best].term())) {
                best = i;
            }
        }
        return best;
    }

    int awaitLeader() {
        runUntil(() -> leader() != -1, 2_000, "a leader");
        return leader();
    }

    RaftNode node(int i) {
        return nodes[i];
    }

    // --- internals ---

    private void start(int i) {
        if (storages[i] instanceof FileStorage old) {
            try {
                old.close();                    // the crashed process's file handles
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }
        storages[i] = disks.open(i);            // files: reload everything from disk
        List<Long> myApplied = applied.get(i);
        StateMachine sm = new StateMachine() {
            @Override
            public void apply(long index, byte[] command) {
                long value = ByteBuffer.wrap(command).getLong();
                Long earlier = appliedAnywhere.putIfAbsent(index, value);
                committedInTerm.putIfAbsent(index, storages[i].currentTerm());
                if (earlier != null && earlier != value) {
                    throw new AssertionError("STATE MACHINE SAFETY: index " + index + " applied as "
                            + earlier + " on one node and " + value + " on node " + i);
                }
                myApplied.add(value);
            }

            /** The state is the list of values applied so far. */
            @Override
            public byte[] snapshot() {
                ByteBuffer b = ByteBuffer.allocate(8 * myApplied.size());
                myApplied.forEach(b::putLong);
                return b.array();
            }

            @Override
            public void restore(byte[] data) {
                myApplied.clear();
                ByteBuffer b = ByteBuffer.wrap(data);
                while (b.hasRemaining()) {
                    myApplied.add(b.getLong());
                }
            }
        };
        Transport t = m -> {
            if (rnd.nextDouble() >= dropRate) {
                network.add(new InFlight(now + 1 + rnd.nextInt(maxDelay), m));
            }
        };
        nodes[i] = new RaftNode(i, size, storages[i], t, sm, new Random(rnd.nextLong()), config);
    }

    private void checkElectionSafety() {
        for (RaftNode n : nodes) {
            if (n != null && n.role() == Role.LEADER) {
                Integer other = leaderOfTerm.putIfAbsent(n.term(), n.id());
                if (other != null && other != n.id()) {
                    throw new AssertionError("ELECTION SAFETY: nodes " + other + " and " + n.id()
                            + " both leader in term " + n.term());
                }
            }
        }
    }

    private void checkCommitNeverDecreases() {
        for (int i = 0; i < size; i++) {
            RaftNode n = nodes[i];
            if (n != null) {
                if (n.commitIndex() < lastCommit[i]) {
                    throw new AssertionError("COMMIT MONOTONIC: node " + i + " commitIndex went from "
                            + lastCommit[i] + " to " + n.commitIndex());
                }
                lastCommit[i] = n.commitIndex();
            }
        }
    }

    /** Same index and term on two nodes ⇒ same entry there and everywhere before. */
    void checkLogMatching() {
        for (int a = 0; a < size; a++) {
            for (int b = a + 1; b < size; b++) {
                Storage x = storages[a], y = storages[b];
                long last = Math.min(x.lastIndex(), y.lastIndex());
                long floor = Math.max(x.snapshotIndex(), y.snapshotIndex());   // compacted below
                boolean matchedAbove = false;
                for (long i = last; i > floor; i--) {
                    boolean sameTerm = x.termAt(i) == y.termAt(i);
                    if (matchedAbove && !sameTerm) {
                        throw new AssertionError("LOG MATCHING: nodes " + a + "," + b
                                + " agree above index " + i + " but not at it");
                    }
                    if (sameTerm) {
                        matchedAbove = true;
                        if (!Arrays.equals(x.entry(i).command(), y.entry(i).command())) {
                            throw new AssertionError("LOG MATCHING: nodes " + a + "," + b
                                    + " same term at index " + i + " but different commands");
                        }
                    }
                }
            }
        }
    }

    /**
     * Leader completeness: an entry committed in term T is in the log of
     * every leader of term T or later. A stale leader of an older term (cut
     * off in a minority partition) is allowed to lack it, and is skipped.
     */
    void checkLeaderCompleteness() {
        for (RaftNode n : nodes) {
            if (n == null || n.role() != Role.LEADER) {
                continue;
            }
            for (Map.Entry<Long, Long> e : appliedAnywhere.entrySet()) {
                long index = e.getKey();
                if (n.term() < committedInTerm.get(index) || index <= n.snapshotIndex()) {
                    continue;                   // stale leader, or inside its snapshot
                }
                if (index > n.lastIndex()
                        || n.entry(index).command().length != 8
                        || ByteBuffer.wrap(n.entry(index).command()).getLong() != e.getValue()) {
                    throw new AssertionError("LEADER COMPLETENESS: leader " + n.id() + " (term "
                            + n.term() + ") lacks committed index " + index);
                }
            }
        }
    }

    /** Releases file handles (file-backed clusters). */
    void closeAll() throws java.io.IOException {
        for (Storage st : storages) {
            if (st instanceof FileStorage f) {
                f.close();
            }
        }
    }

    int committedCount() {
        return appliedAnywhere.size();
    }
}
