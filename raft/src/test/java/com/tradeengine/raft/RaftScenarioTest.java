package com.tradeengine.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** docs/lld-raft.html §6, scenario tests. Safety is checked on every step too. */
class RaftScenarioTest {

    @Test
    void electsExactlyOneLeader() {
        SimCluster c = new SimCluster(3, 1);
        int leader = c.awaitLeader();
        c.run(200);                             // stays stable with a healthy network

        assertEquals(leader, c.leader());
        for (int i = 0; i < 3; i++) {
            assertEquals(leader, c.node(i).leaderId(), "node " + i + " follows the leader");
            assertEquals(c.node(leader).term(), c.node(i).term());
        }
    }

    @Test
    void commandsReachEveryNodeInOrder() {
        SimCluster c = new SimCluster(3, 2);
        int leader = c.awaitLeader();
        for (int i = 0; i < 10; i++) {
            c.propose(leader);
        }
        c.runUntil(() -> allApplied(c, 10), 500, "10 commands everywhere");

        List<Long> expected = List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
        for (int i = 0; i < 3; i++) {
            assertEquals(expected, c.applied.get(i), "node " + i);
        }
    }

    @Test
    void followerRefusesToAcceptCommands() {
        SimCluster c = new SimCluster(3, 3);
        int leader = c.awaitLeader();
        int follower = (leader + 1) % 3;
        assertEquals(-1, c.propose(follower));
    }

    @Test
    void leaderCrashElectsNewLeaderAndKeepsCommittedCommands() {
        SimCluster c = new SimCluster(3, 4);
        int first = c.awaitLeader();
        long firstTerm = c.node(first).term();
        for (int i = 0; i < 5; i++) {
            c.propose(first);
        }
        c.runUntil(() -> allApplied(c, 5), 500, "5 committed");

        c.crash(first);
        int second = c.awaitLeader();
        assertNotEquals(first, second);
        assertTrue(c.node(second).term() > firstTerm, "new leader is in a higher term");

        c.propose(second);
        c.restart(first);
        c.runUntil(() -> allApplied(c, 6), 1_000, "6 committed everywhere, old leader too");
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L), c.applied.get(first));
    }

    @Test
    void laggingFollowerCatchesUp() {
        SimCluster c = new SimCluster(3, 5);
        int leader = c.awaitLeader();
        int slow = (leader + 1) % 3;
        c.isolate(slow);
        for (int i = 0; i < 40; i++) {
            c.propose(leader);                  // committed by leader + the other follower
        }
        c.run(100);
        assertEquals(0, c.applied.get(slow).size());

        c.heal();
        c.runUntil(() -> c.applied.get(slow).size() == 40, 1_000, "slow follower caught up");
    }

    @Test
    void isolatedLeaderCannotCommitAndItsEntriesAreReplaced() {
        SimCluster c = new SimCluster(3, 6);
        int old = c.awaitLeader();
        c.propose(old);
        c.runUntil(() -> allApplied(c, 1), 500, "first command everywhere");

        c.isolate(old);                         // leader alone: no majority
        for (int i = 0; i < 3; i++) {
            c.propose(old, 900 + i);            // accepted into its log, never committed
        }
        c.run(50);
        assertEquals(1, c.applied.get(old).size(), "minority side committed nothing");

        // The other two elect a new leader and commit different commands.
        int[] majority = others(old);
        c.runUntil(() -> c.leader() != -1 && c.leader() != old, 2_000, "new leader on majority side");
        int fresh = c.leader();
        for (int i = 0; i < 3; i++) {
            c.propose(fresh, 500 + i);
        }
        c.runUntil(() -> c.applied.get(majority[0]).size() == 4
                && c.applied.get(majority[1]).size() == 4, 1_000, "majority committed");

        c.heal();                               // old leader sees the higher term, steps down
        c.runUntil(() -> c.applied.get(old).size() == 4, 1_000, "old leader converged");
        assertEquals(List.of(1L, 500L, 501L, 502L), c.applied.get(old));
        assertEquals(Role.FOLLOWER, c.node(old).role());
    }

    @Test
    void fiveNodesSurviveTwoFailures() {
        SimCluster c = new SimCluster(5, 7);
        int leader = c.awaitLeader();
        c.crash((leader + 1) % 5);
        c.crash((leader + 2) % 5);
        for (int i = 0; i < 5; i++) {
            c.propose(leader);
        }
        c.runUntil(() -> c.applied.get(leader).size() == 5, 500, "committed with 3 of 5 up");
    }

    @Test
    void threeOfFiveDownMeansNoProgress() {
        SimCluster c = new SimCluster(5, 8);
        int leader = c.awaitLeader();
        for (int k = 1; k <= 3; k++) {
            c.crash((leader + k) % 5);
        }
        c.propose(leader);
        c.run(300);
        assertEquals(0, c.applied.get(leader).size(), "no majority, nothing may commit");
    }

    @Test
    void singleNodeCommitsAlone() {
        SimCluster c = new SimCluster(1, 9);
        int leader = c.awaitLeader();
        c.propose(leader);
        assertEquals(List.of(1L), c.applied.get(0));
    }

    @Test
    void restartedNodeRemembersItsVote() {
        InMemoryStorage disk = new InMemoryStorage();
        List<Message> sent = new ArrayList<>();
        RaftNode node = new RaftNode(0, 3, disk, sent::add, (i, cmd) -> { },
                new java.util.Random(1), RaftNode.Config.DEFAULT);

        node.handle(new Message.RequestVote(1, 0, 5, 0, 0));
        assertEquals(new Message.VoteResponse(0, 1, 5, true), sent.get(0));

        // Crash and restart from the same disk: a second candidate in the
        // same term must be refused, or term 5 could get two leaders.
        RaftNode again = new RaftNode(0, 3, disk, sent::add, (i, cmd) -> { },
                new java.util.Random(2), RaftNode.Config.DEFAULT);
        again.handle(new Message.RequestVote(2, 0, 5, 0, 0));
        assertEquals(new Message.VoteResponse(0, 2, 5, false), sent.get(1));
    }

    @Test
    void voteRefusedToCandidateWithOlderLog() {
        InMemoryStorage disk = new InMemoryStorage();
        disk.saveTermAndVote(3, -1);
        disk.append(List.of(new LogEntry(3, new byte[8])));
        List<Message> sent = new ArrayList<>();
        RaftNode node = new RaftNode(0, 3, disk, sent::add, (i, cmd) -> { },
                new java.util.Random(1), RaftNode.Config.DEFAULT);

        // Candidate's last entry is from term 2: behind us, however long its log.
        node.handle(new Message.RequestVote(1, 0, 4, 10, 2));
        assertEquals(new Message.VoteResponse(0, 1, 4, false), sent.get(0));
    }

    private static boolean allApplied(SimCluster c, int n) {
        for (int i = 0; i < c.size; i++) {
            if (c.isUp(i) && c.applied.get(i).size() < n) {
                return false;
            }
        }
        return true;
    }

    private static int[] others(int i) {
        return new int[] {(i + 1) % 3, (i + 2) % 3};
    }
}
