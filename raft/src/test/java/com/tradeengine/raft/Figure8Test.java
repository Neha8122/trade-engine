package com.tradeengine.raft;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tradeengine.raft.Message.AppendEntries;
import com.tradeengine.raft.Message.RequestVote;
import com.tradeengine.raft.Message.VoteResponse;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

/**
 * The Raft paper's Figure 8, forced step by step: why a leader must never
 * commit an entry from an older term just because a majority now stores it.
 *
 * <p>Random chaos rarely hits this exact sequence, so here every message
 * and every tick is delivered by hand. Nodes only see time when this test
 * ticks them, so it decides exactly who times out and wins each election.
 *
 * <pre>
 *  (a) S0 leader, term 1: entry A at index 2 reaches only S1
 *  (b) S0 down. S4 wins term 2 with votes of S2, S3; writes index 2 = its
 *      own term-2 entry, which reaches nobody
 *  (c) S4 down, S0 back. S0 wins term 3 with S1, S2 and copies A to S2.
 *      A (term 1) is now on a majority: S0, S1, S2. It must NOT be
 *      committed yet. S0 goes down before its term-3 entry spreads.
 *  (d) S4 back, wins term 4 with S2, S3 (its last term, 2, beats their 1)
 *      and overwrites index 2 everywhere. If S0 had committed A in (c),
 *      a committed entry would now be gone.
 * </pre>
 */
class Figure8Test {

    private static final long A = 111;
    private static final long B = 222;

    private final int size = 5;
    private final InMemoryStorage[] disk = new InMemoryStorage[size];
    private final RaftNode[] node = new RaftNode[size];
    private final boolean[] up = new boolean[size];
    private final List<Message> inFlight = new ArrayList<>();
    private final List<long[]> appliedEver = new ArrayList<>();    // {node, index, value}
    // One entry per message, so the leader's term-3 entry and A travel separately.
    private final RaftNode.Config config = new RaftNode.Config(15, 30, 5, 1);

    @Test
    void oldTermEntryOnAMajorityIsNotCommittedByCounting() {
        for (int i = 0; i < size; i++) {
            disk[i] = new InMemoryStorage();
            start(i);
        }

        // (a) S0 elected in term 1 by everyone; A reaches only S1.
        tickUntil(0, () -> node[0].role() == Role.CANDIDATE);
        deliver(m -> true);
        assertEquals(Role.LEADER, node[0].role());
        node[0].propose(bytes(A));
        deliver(m -> between(m, 0, 1));
        inFlight.clear();
        assertEquals(2, node[1].lastIndex());

        // (b) S0 down; S4 elected in term 2 by S2, S3. Its entries go nowhere.
        crash(0);
        tickUntil(4, () -> node[4].role() == Role.CANDIDATE);
        deliver(m -> isVote(m) && among(m, 2, 3, 4));
        assertEquals(Role.LEADER, node[4].role());
        assertEquals(2, node[4].term());
        node[4].propose(bytes(B));
        inFlight.clear();

        // (c) S4 down, S0 back; S0 needs term 3 (S2 already voted in term 2).
        crash(4);
        start(0);
        tickUntil(0, () -> node[0].role() == Role.CANDIDATE && node[0].term() >= 3);
        inFlight.removeIf(m -> m.term() < 3);
        deliver(m -> isVote(m) && among(m, 0, 1, 2));
        assertEquals(Role.LEADER, node[0].role());
        // Replicate among S0, S1, S2, but never let S2 accept S0's term-3
        // entry, so A (term 1) sits on a majority while the term-3 entry
        // doesn't. Probes S2 will reject (it lacks the previous entry) still
        // go through: that's what makes S0 step back and send A.
        deliver(m -> among(m, 0, 1, 2) && !(m instanceof AppendEntries ae
                && ae.to() == 2 && node[2].lastIndex() >= ae.prevLogIndex()
                && ae.entries().stream().anyMatch(e -> e.term() == 3)));
        assertEquals(2, node[2].lastIndex(), "A reached S2");
        assertEquals(1, node[0].termAt(2));
        // (S0 restarted in this step, so it may not even know index 1 is
        // committed yet; what matters is that A, at index 2, is not.)
        assertTrue(node[0].commitIndex() < 2,
                "A is on a majority but from an old term: counting replicas must not commit it");
        crash(0);

        // (d) S4 back and elected in term 4 by S2, S3; it overwrites index 2.
        start(4);
        tickUntil(4, () -> node[4].role() == Role.CANDIDATE && node[4].term() >= 4);
        inFlight.removeIf(m -> m.term() < 4);
        deliver(m -> isVote(m) && among(m, 2, 3, 4));
        assertEquals(Role.LEADER, node[4].role());
        deliver(m -> among(m, 2, 3, 4));
        // Followers learn the new commit index from the next heartbeat.
        for (int t = 0; t < config.heartbeatTicks(); t++) {
            node[4].tick();
        }
        deliver(m -> among(m, 2, 3, 4));

        // Nothing ever applied anywhere may disagree with the final leader.
        for (long[] a : appliedEver) {
            assertArrayEquals(bytes(a[2]), node[4].entry(a[1]).command(),
                    "node " + a[0] + " applied " + a[2] + " at index " + a[1]
                    + ", but the new leader's log has something else there: a committed entry was lost");
        }
        assertEquals(List.of(B), appliedBy(2));
    }

    // --- harness: time and the network move only when the test says ---

    private void start(int i) {
        int id = i;
        up[i] = true;
        node[i] = new RaftNode(i, size, disk[i], inFlight::add,
                (index, cmd) -> appliedEver.add(new long[] {id, index, ByteBuffer.wrap(cmd).getLong()}),
                new Random(i), config);
    }

    private void crash(int i) {
        up[i] = false;
        inFlight.removeIf(m -> m.from() == i || m.to() == i);
    }

    private void tickUntil(int i, BooleanSupplier done) {
        for (int t = 0; t < 1_000 && !done.getAsBoolean(); t++) {
            node[i].tick();
        }
        if (!done.getAsBoolean()) {
            throw new AssertionError("node " + i + " never reached the wanted state");
        }
    }

    /** Delivers matching messages one at a time, oldest first, until none match. */
    private void deliver(Predicate<Message> which) {
        while (true) {
            Message next = null;
            for (Message m : inFlight) {
                if (up[m.from()] && up[m.to()] && which.test(m)) {
                    next = m;
                    break;
                }
            }
            if (next == null) {
                return;
            }
            inFlight.remove(next);
            node[next.to()].handle(next);
        }
    }

    private static boolean between(Message m, int a, int b) {
        return (m.from() == a && m.to() == b) || (m.from() == b && m.to() == a);
    }

    private static boolean among(Message m, int... ids) {
        boolean from = false, to = false;
        for (int id : ids) {
            from |= m.from() == id;
            to |= m.to() == id;
        }
        return from && to;
    }

    private static boolean isVote(Message m) {
        return m instanceof RequestVote || m instanceof VoteResponse;
    }

    private List<Long> appliedBy(int i) {
        List<Long> out = new ArrayList<>();
        for (long[] a : appliedEver) {
            if (a[0] == i) {
                out.add(a[2]);
            }
        }
        return out;
    }

    private static byte[] bytes(long v) {
        return ByteBuffer.allocate(8).putLong(v).array();
    }
}
