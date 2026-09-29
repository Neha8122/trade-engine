package com.tradeengine.raft;

/**
 * One slot in the replicated log: the term it was created in, and the
 * command bytes. Raft never looks inside the command.
 *
 * <p>An empty command is a no-op, which a new leader appends so it can
 * commit entries left over from earlier terms (docs/lld-raft.html §4).
 */
public record LogEntry(long term, byte[] command) {

    static final byte[] NO_OP = new byte[0];

    public boolean isNoOp() {
        return command.length == 0;
    }
}
