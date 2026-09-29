package com.tradeengine.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;
import java.util.stream.LongStream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Randomised fault injection, the strong test (docs/lld-raft.html §6).
 *
 * <p>Each seed runs a cluster through thousands of ticks of chaos: lost and
 * delayed messages, random partitions, crashes and restarts, and client
 * proposals whenever there is a leader. {@link SimCluster} checks election
 * safety, log matching, leader completeness and state machine safety while
 * it runs. At the end the network is healed and every node must converge on
 * exactly the same sequence of applied commands.
 *
 * <p>A failing seed is printed in the test name and replays exactly.
 */
class RaftSimulationTest {

    static LongStream seeds() {
        return LongStream.rangeClosed(1, 200);
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void survivesChaos(long seed) {
        int size = seed % 2 == 0 ? 5 : 3;
        SimCluster c = new SimCluster(size, seed);
        c.dropRate = 0.10;
        c.maxDelay = 6;                         // delays up to 6 ticks: heavy reordering
        Random chaos = new Random(seed * 31);

        for (int t = 0; t < 3_000; t++) {
            int r = chaos.nextInt(1_000);
            if (r < 300) {
                int leader = c.leader();
                if (leader != -1) {
                    c.propose(leader);
                }
            } else if (r < 303) {
                c.crash(chaos.nextInt(size));
            } else if (r < 315) {
                c.restart(chaos.nextInt(size));
            } else if (r < 320) {
                for (int i = 0; i < size; i++) {
                    c.setPartition(i, chaos.nextInt(2));    // random split into two sides
                }
            } else if (r < 330) {
                c.heal();
            }
            c.step();
        }

        // Calm down: everyone up, network perfect, until all agree.
        c.heal();
        c.dropRate = 0;
        c.maxDelay = 1;
        for (int i = 0; i < size; i++) {
            c.restart(i);
        }
        int leader = c.awaitLeader();
        c.propose(leader);                      // a final command in the leader's term
        c.runUntil(() -> converged(c), 5_000, "all nodes applied the same log");
        c.checkLogMatching();
        c.checkLeaderCompleteness();

        assertTrue(c.committedCount() > 10, "chaos too heavy to test anything: "
                + c.committedCount() + " commits");
        for (int i = 1; i < size; i++) {
            assertEquals(c.applied.get(0), c.applied.get(i), "node " + i + " vs node 0");
        }
    }

    private static boolean converged(SimCluster c) {
        int leader = c.leader();
        if (leader == -1) {
            return false;
        }
        long commit = c.node(leader).commitIndex();
        if (commit < c.node(leader).lastIndex()) {
            return false;
        }
        for (int i = 0; i < c.size; i++) {
            if (c.node(i).lastApplied() != commit || !c.applied.get(i).equals(c.applied.get(leader))) {
                return false;
            }
        }
        return true;
    }
}
