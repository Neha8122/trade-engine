package com.tradeengine.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** docs/lld-snapshots.html: compaction, InstallSnapshot, restore on restart. */
class RaftSnapshotTest {

    private static final RaftNode.Config SNAP10 = RaftNode.Config.DEFAULT.withSnapshotEvery(10);

    @TempDir
    Path tmp;

    @Test
    void logStaysShortWhileCommandsKeepComing() {
        SimCluster c = new SimCluster(3, 1, SNAP10);
        int leader = c.awaitLeader();
        for (int i = 0; i < 300; i++) {
            c.propose(leader);
            c.step();
        }
        c.runUntil(() -> allHave(c, 300), 2_000, "300 applied everywhere");
        for (int i = 0; i < 3; i++) {
            RaftNode n = c.node(i);
            assertTrue(n.snapshotsTaken() > 20, "node " + i + " took " + n.snapshotsTaken() + " snapshots");
            assertTrue(n.lastIndex() - n.snapshotIndex() < 30,
                    "node " + i + " keeps " + (n.lastIndex() - n.snapshotIndex()) + " entries; the rest is compacted");
        }
    }

    @Test
    void followerTooFarBehindGetsTheSnapshot() {
        SimCluster c = new SimCluster(3, 2, SNAP10);
        int leader = c.awaitLeader();
        int slow = (leader + 1) % 3;
        c.isolate(slow);
        for (int i = 0; i < 100; i++) {
            c.propose(leader);
        }
        c.run(200);
        assertTrue(c.node(leader).snapshotIndex() > 50, "leader compacted what the follower needs");

        c.heal();
        c.runUntil(() -> c.applied.get(slow).size() == 100, 2_000, "slow follower caught up");
        assertTrue(c.node(slow).snapshotsInstalled() >= 1, "caught up through InstallSnapshot");
        assertTrue(c.node(leader).snapshotsSent() >= 1);
        assertEquals(c.applied.get(leader), c.applied.get(slow));
    }

    @Test
    void restartStartsFromTheSnapshotNotFromIndexOne() {
        SimCluster c = new SimCluster(3, 3, SNAP10);
        int leader = c.awaitLeader();
        int other = (leader + 1) % 3;
        for (int i = 0; i < 55; i++) {
            c.propose(leader);
        }
        c.runUntil(() -> allHave(c, 55), 2_000, "55 applied");
        long snapshotAt = c.node(other).snapshotIndex();
        assertTrue(snapshotAt >= 50);

        c.crash(other);
        c.restart(other);
        // Before any message arrives, the restarted node already holds
        // everything up to its snapshot.
        assertEquals(snapshotAt, c.node(other).lastApplied());
        assertTrue(c.applied.get(other).size() >= 50);
        c.runUntil(() -> c.applied.get(other).size() == 55, 1_000, "caught up the rest");
        assertEquals(c.applied.get(leader), c.applied.get(other));
    }

    static LongStream seeds() {
        return LongStream.rangeClosed(1, 100);
    }

    /** The same chaos as RaftSimulationTest, snapshotting every 20 entries. */
    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void survivesChaosWithSnapshots(long seed) {
        chaos(new SimCluster(seed % 2 == 0 ? 5 : 3, seed, RaftNode.Config.DEFAULT.withSnapshotEvery(20)),
                seed, 3_000);
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {201, 202, 203, 204, 205})
    void survivesChaosWithSnapshotsOnRealFiles(long seed) throws Exception {
        Path dir = tmp.resolve("seed-" + seed);
        SimCluster c = new SimCluster(3, seed, RaftNode.Config.DEFAULT.withSnapshotEvery(20),
                node -> new FileStorage(dir.resolve("node-" + node)));
        chaos(c, seed, 1_000);
        c.closeAll();
    }

    private static void chaos(SimCluster c, long seed, int steps) {
        c.dropRate = 0.10;
        c.maxDelay = 6;
        Random chaos = new Random(seed * 31);
        for (int t = 0; t < steps; t++) {
            int r = chaos.nextInt(1_000);
            if (r < 300) {
                int leader = c.leader();
                if (leader != -1) {
                    c.propose(leader);
                }
            } else if (r < 306) {
                c.crash(chaos.nextInt(c.size));
            } else if (r < 330) {
                c.restart(chaos.nextInt(c.size));
            } else if (r < 335) {
                for (int i = 0; i < c.size; i++) {
                    c.setPartition(i, chaos.nextInt(2));
                }
            } else if (r < 345) {
                c.heal();
            }
            c.step();
        }
        c.heal();
        c.dropRate = 0;
        c.maxDelay = 1;
        for (int i = 0; i < c.size; i++) {
            c.restart(i);
        }
        int leader = c.awaitLeader();
        c.propose(leader);
        c.runUntil(() -> {
            int l = c.leader();
            if (l == -1 || c.node(l).commitIndex() < c.node(l).lastIndex()) {
                return false;
            }
            for (int i = 0; i < c.size; i++) {
                if (c.node(i).lastApplied() != c.node(l).commitIndex()) {
                    return false;
                }
            }
            return true;
        }, 5_000, "all nodes applied the same log");
        c.checkLogMatching();
        c.checkLeaderCompleteness();

        assertTrue(c.committedCount() > 10);
        List<Long> first = new ArrayList<>(c.applied.get(0));
        for (int i = 1; i < c.size; i++) {
            assertEquals(first, c.applied.get(i), "node " + i + " vs node 0");
        }
    }

    private static boolean allHave(SimCluster c, int n) {
        for (int i = 0; i < c.size; i++) {
            if (c.applied.get(i).size() < n) {
                return false;
            }
        }
        return true;
    }
}
