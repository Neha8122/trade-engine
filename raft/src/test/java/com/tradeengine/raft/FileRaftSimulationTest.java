package com.tradeengine.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Random;
import java.util.stream.LongStream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The chaos simulation again, but every node keeps its state in real files
 * with fsync. A crash abandons the node without any shutdown; a restart
 * builds a new {@link FileStorage} that reloads term, vote and log from
 * disk. Same safety checks, same convergence requirement.
 */
class FileRaftSimulationTest {

    @TempDir
    Path root;

    static LongStream seeds() {
        return LongStream.rangeClosed(1, 10);
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void survivesChaosOnRealFiles(long seed) throws Exception {
        int size = 3;
        Path dir = root.resolve("seed-" + seed);
        SimCluster c = new SimCluster(size, seed, RaftNode.Config.DEFAULT,
                node -> new FileStorage(dir.resolve("node-" + node)));
        c.dropRate = 0.10;
        c.maxDelay = 6;
        Random chaos = new Random(seed * 31);
        int restarts = 0;

        for (int t = 0; t < 1_000; t++) {
            int r = chaos.nextInt(1_000);
            if (r < 300) {
                int leader = c.leader();
                if (leader != -1) {
                    c.propose(leader);
                }
            } else if (r < 312) {             // ~1% of ticks: a node dies
                c.crash(chaos.nextInt(size));
            } else if (r < 340) {
                int i = chaos.nextInt(size);
                if (!c.isUp(i)) {
                    restarts++;
                }
                c.restart(i);
            }
            c.step();
        }

        c.heal();
        c.dropRate = 0;
        c.maxDelay = 1;
        for (int i = 0; i < size; i++) {
            c.restart(i);
        }
        int leader = c.awaitLeader();
        c.propose(leader);
        c.runUntil(() -> {
            int l = c.leader();
            if (l == -1 || c.node(l).commitIndex() < c.node(l).lastIndex()) {
                return false;
            }
            for (int i = 0; i < size; i++) {
                if (!c.applied.get(i).equals(c.applied.get(l))) {
                    return false;
                }
            }
            return true;
        }, 5_000, "all nodes applied the same log");
        c.closeAll();

        assertTrue(restarts > 3, "too few restarts from disk: " + restarts);
        assertTrue(c.committedCount() > 10);
        for (int i = 1; i < size; i++) {
            assertEquals(c.applied.get(0), c.applied.get(i));
        }
    }
}
