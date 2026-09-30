package com.tradeengine.server;

import com.tradeengine.raft.LogEntry;
import com.tradeengine.raft.Message;
import com.tradeengine.raft.Message.AppendEntries;
import com.tradeengine.raft.Message.AppendResponse;
import com.tradeengine.raft.Message.RequestVote;
import com.tradeengine.raft.Message.VoteResponse;
import java.net.ProtocolException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Raft messages between servers, one frame each. A node that opens a
 * connection first sends {@code HELLO} with its node id, so the other side
 * knows which peer the connection belongs to.
 */
public final class PeerProtocol {

    private PeerProtocol() { }

    public static final byte HELLO = 'H';
    static final byte REQUEST_VOTE = 'V';
    static final byte VOTE_RESPONSE = 'v';
    static final byte APPEND_ENTRIES = 'E';
    static final byte APPEND_RESPONSE = 'e';

    public static void hello(FrameWriter w, int nodeId) {
        w.begin(HELLO, 4).putInt(nodeId);
        w.end();
    }

    public static int helloNodeId(ByteBuffer b, int at) {
        return b.getInt(at + 1);
    }

    public static void encode(FrameWriter w, Message m) {
        switch (m) {
            case RequestVote rv -> {
                w.begin(REQUEST_VOTE, 32).putInt(rv.from()).putInt(rv.to()).putLong(rv.term())
                        .putLong(rv.lastLogIndex()).putLong(rv.lastLogTerm());
                w.end();
            }
            case VoteResponse vr -> {
                w.begin(VOTE_RESPONSE, 17).putInt(vr.from()).putInt(vr.to()).putLong(vr.term())
                        .put((byte) (vr.granted() ? 1 : 0));
                w.end();
            }
            case AppendEntries ae -> {
                int size = 44;
                for (LogEntry e : ae.entries()) {
                    size += 12 + e.command().length;
                }
                ByteBuffer b = w.begin(APPEND_ENTRIES, size).putInt(ae.from()).putInt(ae.to())
                        .putLong(ae.term()).putLong(ae.prevLogIndex()).putLong(ae.prevLogTerm())
                        .putLong(ae.leaderCommit()).putInt(ae.entries().size());
                for (LogEntry e : ae.entries()) {
                    b.putLong(e.term()).putInt(e.command().length).put(e.command());
                }
                w.end();
            }
            case AppendResponse ar -> {
                w.begin(APPEND_RESPONSE, 25).putInt(ar.from()).putInt(ar.to()).putLong(ar.term())
                        .put((byte) (ar.success() ? 1 : 0)).putLong(ar.matchIndex());
                w.end();
            }
        }
    }

    /** Decodes a frame; {@code at} is the type byte. */
    public static Message decode(ByteBuffer b, int at, int length) throws ProtocolException {
        byte type = b.get(at);
        int from = b.getInt(at + 1);
        int to = b.getInt(at + 5);
        long term = b.getLong(at + 9);
        switch (type) {
            case REQUEST_VOTE:
                return new RequestVote(from, to, term, b.getLong(at + 17), b.getLong(at + 25));
            case VOTE_RESPONSE:
                return new VoteResponse(from, to, term, b.get(at + 17) == 1);
            case APPEND_RESPONSE:
                return new AppendResponse(from, to, term, b.get(at + 17) == 1, b.getLong(at + 18));
            case APPEND_ENTRIES: {
                long prevIndex = b.getLong(at + 17);
                long prevTerm = b.getLong(at + 25);
                long leaderCommit = b.getLong(at + 33);
                int count = b.getInt(at + 41);
                List<LogEntry> entries = new ArrayList<>(count);
                int p = at + 45;
                for (int i = 0; i < count; i++) {
                    long entryTerm = b.getLong(p);
                    int len = b.getInt(p + 8);
                    byte[] command = new byte[len];
                    b.get(p + 12, command);
                    entries.add(new LogEntry(entryTerm, command));
                    p += 12 + len;
                }
                if (p != at + length) {
                    throw new ProtocolException("AppendEntries length mismatch");
                }
                return new AppendEntries(from, to, term, prevIndex, prevTerm, entries, leaderCommit);
            }
            default:
                throw new ProtocolException("unknown peer message type " + type);
        }
    }
}
