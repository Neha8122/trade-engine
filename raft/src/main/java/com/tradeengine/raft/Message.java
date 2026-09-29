package com.tradeengine.raft;

import java.util.List;

/**
 * The four messages Raft nodes exchange. Every message carries the sender's
 * term: a node that sees a higher term than its own steps down at once.
 */
public sealed interface Message {

    int from();
    int to();
    long term();

    /** Candidate asking for a vote. */
    record RequestVote(int from, int to, long term, long lastLogIndex, long lastLogTerm)
            implements Message { }

    record VoteResponse(int from, int to, long term, boolean granted)
            implements Message { }

    /**
     * Leader sending entries that follow {@code prevLogIndex}; empty entries
     * make it a heartbeat. The follower accepts only if its entry at
     * {@code prevLogIndex} has {@code prevLogTerm}.
     */
    record AppendEntries(int from, int to, long term, long prevLogIndex, long prevLogTerm,
                         List<LogEntry> entries, long leaderCommit) implements Message { }

    /**
     * @param matchIndex on success, the follower's log matches the leader's
     *                   up to here; on failure, the follower's last index,
     *                   as a hint for where the leader should retry
     */
    record AppendResponse(int from, int to, long term, boolean success, long matchIndex)
            implements Message { }
}
