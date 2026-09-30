package com.tradeengine.raft;

import com.tradeengine.raft.Message.AppendEntries;
import com.tradeengine.raft.Message.AppendResponse;
import com.tradeengine.raft.Message.InstallSnapshot;
import com.tradeengine.raft.Message.RequestVote;
import com.tradeengine.raft.Message.VoteResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * One Raft node (docs/lld-raft.html). Pure logic: it never reads a clock,
 * never starts a thread and never touches a socket. Time arrives as
 * {@link #tick()}, messages as {@link #handle(Message)}, commands as
 * {@link #propose(byte[])}; everything it decides goes out through
 * {@link Transport}, {@link Storage} and {@link StateMachine}.
 *
 * <p>That is what makes it testable: a simulator can run thousands of
 * elections, partitions and crashes per second and replay any failure
 * exactly from its random seed. All methods must be called from one thread.
 *
 * <p>Nodes are numbered 0 .. clusterSize-1.
 */
public final class RaftNode {

    /**
     * Timing, in ticks (with 10 ms ticks: election 150–300 ms, heartbeat
     * 50 ms), and how often to snapshot: every {@code snapshotEvery} applied
     * entries, or never if 0.
     */
    public record Config(int electionTicksMin, int electionTicksMax, int heartbeatTicks,
                         int maxEntriesPerMessage, int snapshotEvery) {
        public static final Config DEFAULT = new Config(15, 30, 5, 256, 0);

        public Config(int electionTicksMin, int electionTicksMax, int heartbeatTicks, int maxEntriesPerMessage) {
            this(electionTicksMin, electionTicksMax, heartbeatTicks, maxEntriesPerMessage, 0);
        }

        public Config withSnapshotEvery(int entries) {
            return new Config(electionTicksMin, electionTicksMax, heartbeatTicks, maxEntriesPerMessage, entries);
        }
    }

    private final int id;
    private final int clusterSize;
    private final Storage storage;
    private final Transport transport;
    private final StateMachine stateMachine;
    private final Random random;
    private final Config config;

    private Role role = Role.FOLLOWER;
    private int leaderId = -1;
    private long commitIndex;
    private long lastApplied;

    private int ticksSinceReset;
    private int electionTimeout;
    private long snapshotsTaken;
    private long snapshotsSent;
    private long snapshotsInstalled;

    // Candidate state
    private final boolean[] votesGranted;

    // Leader state, per node: next entry to send, highest entry known to match
    private final long[] nextIndex;
    private final long[] matchIndex;

    public RaftNode(int id, int clusterSize, Storage storage, Transport transport,
                    StateMachine stateMachine, Random random, Config config) {
        this.id = id;
        this.clusterSize = clusterSize;
        this.storage = storage;
        this.transport = transport;
        this.stateMachine = stateMachine;
        this.random = random;
        this.config = config;
        this.votesGranted = new boolean[clusterSize];
        this.nextIndex = new long[clusterSize];
        this.matchIndex = new long[clusterSize];
        // Restarting with a snapshot on disk: start from it, not from index 1.
        if (storage.snapshotIndex() > 0) {
            stateMachine.restore(storage.snapshotData());
            commitIndex = lastApplied = storage.snapshotIndex();
        }
        resetElectionTimer();
    }

    // --- inputs ---

    /** Time moved on by one tick. */
    public void tick() {
        ticksSinceReset++;
        if (role == Role.LEADER) {
            if (ticksSinceReset >= config.heartbeatTicks()) {
                ticksSinceReset = 0;
                broadcastAppend();              // heartbeat, plus any unsent entries
            }
        } else if (ticksSinceReset >= electionTimeout) {
            startElection();
        }
    }

    /**
     * Adds a command to the log if this node is the leader.
     *
     * @return the log index it will be committed at, or -1 if not the leader
     *         (the caller should retry at {@link #leaderId()})
     */
    public long propose(byte[] command) {
        return proposeAll(List.of(command));
    }

    /**
     * Group commit: adds a whole batch of commands with one storage append
     * (one fsync) and one round of AppendEntries, instead of one per command.
     *
     * @return the log index of the first command (the rest follow in order),
     *         or -1 if not the leader
     */
    public long proposeAll(List<byte[]> commands) {
        if (role != Role.LEADER || commands.isEmpty()) {
            return -1;
        }
        long term = storage.currentTerm();
        List<LogEntry> batch = new ArrayList<>(commands.size());
        for (byte[] c : commands) {
            batch.add(new LogEntry(term, c));
        }
        long first = storage.lastIndex() + 1;
        storage.append(batch);
        if (clusterSize == 1) {
            advanceCommitIndex();
        } else {
            broadcastAppend();
        }
        return first;
    }

    public void handle(Message m) {
        if (m.term() > storage.currentTerm()) {
            // Someone is in a newer term: whatever we were, we're now a
            // follower in that term, with no vote cast yet.
            becomeFollower(m.term(), -1);
        }
        switch (m) {
            case RequestVote rv -> onRequestVote(rv);
            case VoteResponse vr -> onVoteResponse(vr);
            case AppendEntries ae -> onAppendEntries(ae);
            case AppendResponse ar -> onAppendResponse(ar);
            case InstallSnapshot is -> onInstallSnapshot(is);
        }
    }

    // --- elections ---

    private void startElection() {
        role = Role.CANDIDATE;
        leaderId = -1;
        long term = storage.currentTerm() + 1;
        storage.saveTermAndVote(term, id);      // durable before asking anyone
        Arrays.fill(votesGranted, false);
        votesGranted[id] = true;
        resetElectionTimer();                   // new random timeout: split votes resolve
        if (clusterSize == 1) {
            becomeLeader();
            return;
        }
        long lastIndex = storage.lastIndex();
        long lastTerm = storage.termAt(lastIndex);
        for (int peer = 0; peer < clusterSize; peer++) {
            if (peer != id) {
                transport.send(new RequestVote(id, peer, term, lastIndex, lastTerm));
            }
        }
    }

    private void onRequestVote(RequestVote m) {
        long term = storage.currentTerm();
        boolean grant = m.term() == term
                && (storage.votedFor() == -1 || storage.votedFor() == m.from())
                && candidateLogIsUpToDate(m.lastLogTerm(), m.lastLogIndex());
        if (grant) {
            storage.saveTermAndVote(term, m.from());   // durable before replying
            resetElectionTimer();               // a live election: don't start our own
        }
        transport.send(new VoteResponse(id, m.from(), term, grant));
    }

    /**
     * The vote rule that keeps committed entries safe: never vote for a
     * candidate whose log is behind ours. Committed entries are on a
     * majority, any two majorities overlap, so a winner always has them.
     */
    private boolean candidateLogIsUpToDate(long lastLogTerm, long lastLogIndex) {
        long myLastIndex = storage.lastIndex();
        long myLastTerm = storage.termAt(myLastIndex);
        return lastLogTerm > myLastTerm
                || (lastLogTerm == myLastTerm && lastLogIndex >= myLastIndex);
    }

    private void onVoteResponse(VoteResponse m) {
        if (role != Role.CANDIDATE || m.term() != storage.currentTerm() || !m.granted()) {
            return;
        }
        votesGranted[m.from()] = true;
        int votes = 0;
        for (boolean v : votesGranted) {
            if (v) {
                votes++;
            }
        }
        if (votes > clusterSize / 2) {
            becomeLeader();
        }
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = id;
        long next = storage.lastIndex() + 1;
        Arrays.fill(nextIndex, next);
        Arrays.fill(matchIndex, 0);
        // A no-op in our own term: once it commits, every earlier entry is
        // committed with it. A leader must never count replicas of an
        // old-term entry to commit it directly (Raft paper, Figure 8).
        storage.append(List.of(new LogEntry(storage.currentTerm(), LogEntry.NO_OP)));
        ticksSinceReset = 0;
        if (clusterSize == 1) {
            advanceCommitIndex();
        } else {
            broadcastAppend();
        }
    }

    private void becomeFollower(long term, int leader) {
        if (term > storage.currentTerm()) {
            storage.saveTermAndVote(term, -1);
        }
        role = Role.FOLLOWER;
        leaderId = leader;
        resetElectionTimer();
    }

    private void resetElectionTimer() {
        ticksSinceReset = 0;
        electionTimeout = config.electionTicksMin()
                + random.nextInt(config.electionTicksMax() - config.electionTicksMin() + 1);
    }

    // --- replication: leader side ---

    private void broadcastAppend() {
        for (int peer = 0; peer < clusterSize; peer++) {
            if (peer != id) {
                sendAppend(peer);
            }
        }
    }

    private void sendAppend(int peer) {
        long prevIndex = nextIndex[peer] - 1;
        if (prevIndex < storage.snapshotIndex()) {
            // The entries this follower needs were compacted: send the state instead.
            transport.send(new InstallSnapshot(id, peer, storage.currentTerm(), storage.snapshotIndex(),
                    storage.snapshotTerm(), storage.snapshotData()));
            nextIndex[peer] = storage.snapshotIndex() + 1;
            snapshotsSent++;
            return;
        }
        long last = Math.min(storage.lastIndex(), prevIndex + config.maxEntriesPerMessage());
        List<LogEntry> entries = new ArrayList<>((int) (last - prevIndex));
        for (long i = prevIndex + 1; i <= last; i++) {
            entries.add(storage.entry(i));
        }
        // Pipelining: assume these arrive and send the next ones straight
        // after, instead of re-sending everything in flight until the reply
        // comes back. A rejection moves nextIndex back again.
        nextIndex[peer] = last + 1;
        transport.send(new AppendEntries(id, peer, storage.currentTerm(), prevIndex,
                storage.termAt(prevIndex), entries, commitIndex));
    }

    private void onAppendResponse(AppendResponse m) {
        if (role != Role.LEADER || m.term() != storage.currentTerm()) {
            return;                             // stale reply from an older term
        }
        int peer = m.from();
        if (m.success()) {
            // max(): replies can arrive out of order; never move backwards
            matchIndex[peer] = Math.max(matchIndex[peer], m.matchIndex());
            nextIndex[peer] = Math.max(nextIndex[peer], matchIndex[peer] + 1);
            advanceCommitIndex();
            if (nextIndex[peer] <= storage.lastIndex()) {
                sendAppend(peer);               // still behind: keep streaming
            }
        } else {
            // Logs disagree at nextIndex-1. Step back, jumping straight to
            // just past the follower's end if its log is shorter.
            nextIndex[peer] = Math.max(1, Math.min(nextIndex[peer] - 1, m.matchIndex() + 1));
            sendAppend(peer);
        }
    }

    /**
     * Commit the highest entry that is (1) from our own term and (2) stored
     * on a majority, counting ourselves.
     */
    private void advanceCommitIndex() {
        for (long n = storage.lastIndex(); n > commitIndex; n--) {
            if (storage.termAt(n) != storage.currentTerm()) {
                break;                          // older terms: only via our own entries
            }
            int replicas = 1;
            for (int peer = 0; peer < clusterSize; peer++) {
                if (peer != id && matchIndex[peer] >= n) {
                    replicas++;
                }
            }
            if (replicas > clusterSize / 2) {
                commitIndex = n;
                applyCommitted();
                return;
            }
        }
    }

    // --- replication: follower side ---

    private void onAppendEntries(AppendEntries m) {
        long term = storage.currentTerm();
        if (m.term() < term) {
            transport.send(new AppendResponse(id, m.from(), term, false, storage.lastIndex()));
            return;                             // from a deposed leader
        }
        // m.term() == term: this is the legitimate leader for our term.
        role = Role.FOLLOWER;
        leaderId = m.from();
        resetElectionTimer();

        long prev = m.prevLogIndex();
        List<LogEntry> entries = m.entries();
        if (prev < storage.snapshotIndex()) {
            // Part of this message is already inside our snapshot. Those
            // entries are committed, so they match the leader's: skip them
            // and continue from the snapshot's last entry.
            long skip = storage.snapshotIndex() - prev;
            if (skip >= entries.size()) {
                transport.send(new AppendResponse(id, m.from(), term, true, prev + entries.size()));
                return;
            }
            entries = entries.subList((int) skip, entries.size());
            prev = storage.snapshotIndex();
        } else if (prev > storage.lastIndex() || storage.termAt(prev) != m.prevLogTerm()) {
            transport.send(new AppendResponse(id, m.from(), term, false,
                    Math.min(storage.lastIndex(), prev - 1)));
            return;
        }

        // Our log matches the leader's up to prev. Keep entries we already
        // have with the same term; at the first conflict, cut our log there
        // (those entries were never committed) and take the leader's.
        int i = 0;
        for (; i < entries.size(); i++) {
            long index = prev + 1 + i;
            if (index > storage.lastIndex()) {
                break;
            }
            if (storage.termAt(index) != entries.get(i).term()) {
                storage.truncateFrom(index);
                break;
            }
        }
        if (i < entries.size()) {
            storage.append(entries.subList(i, entries.size()));   // durable before replying
        }

        long lastNew = prev + entries.size();
        // Only entries this message proved we share with the leader can be
        // marked committed. max(): a delayed old message must never move
        // commitIndex backwards.
        long canCommit = Math.min(m.leaderCommit(), lastNew);
        if (canCommit > commitIndex) {
            commitIndex = canCommit;
            applyCommitted();
        }
        transport.send(new AppendResponse(id, m.from(), term, true, lastNew));
    }

    private void onInstallSnapshot(InstallSnapshot m) {
        long term = storage.currentTerm();
        if (m.term() < term) {
            transport.send(new AppendResponse(id, m.from(), term, false, storage.lastIndex()));
            return;
        }
        role = Role.FOLLOWER;
        leaderId = m.from();
        resetElectionTimer();
        if (m.lastIncludedIndex() > commitIndex) {
            // Keeps our entries after it if they agree, else drops our log.
            storage.installSnapshot(m.lastIncludedIndex(), m.lastIncludedTerm(), m.data());
            stateMachine.restore(m.data());
            commitIndex = lastApplied = m.lastIncludedIndex();
            snapshotsInstalled++;
        }
        transport.send(new AppendResponse(id, m.from(), term, true, m.lastIncludedIndex()));
    }

    // --- applying ---

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry e = storage.entry(lastApplied);
            if (!e.isNoOp()) {
                stateMachine.apply(lastApplied, e.command());
            }
        }
        maybeSnapshot();
    }

    /** Every snapshotEvery applied entries: save the state, drop the log up to it. */
    private void maybeSnapshot() {
        int every = config.snapshotEvery();
        if (every > 0 && lastApplied - storage.snapshotIndex() >= every) {
            storage.installSnapshot(lastApplied, storage.termAt(lastApplied), stateMachine.snapshot());
            snapshotsTaken++;
        }
    }

    // --- read-only views ---

    public int id() { return id; }
    public Role role() { return role; }
    public long term() { return storage.currentTerm(); }
    /** The leader this node believes in, or -1 if unknown. */
    public int leaderId() { return leaderId; }
    public long commitIndex() { return commitIndex; }
    public long lastApplied() { return lastApplied; }
    public long lastIndex() { return storage.lastIndex(); }
    public long termAt(long index) { return storage.termAt(index); }
    public LogEntry entry(long index) { return storage.entry(index); }
    public long snapshotIndex() { return storage.snapshotIndex(); }
    public long snapshotsTaken() { return snapshotsTaken; }
    public long snapshotsSent() { return snapshotsSent; }
    public long snapshotsInstalled() { return snapshotsInstalled; }
}
