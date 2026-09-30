package com.tradeengine.exchange;

import com.tradeengine.orderbook.ExecutionListener;
import com.tradeengine.orderbook.OrderBook;
import com.tradeengine.orderbook.OrderType;
import com.tradeengine.orderbook.Side;
import com.tradeengine.raft.Message;
import com.tradeengine.raft.RaftNode;
import com.tradeengine.raft.Role;
import com.tradeengine.raft.StateMachine;
import com.tradeengine.raft.Storage;
import com.tradeengine.raft.Transport;
import com.tradeengine.orderbook.OrderType;
import com.tradeengine.orderbook.Side;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * One exchange server: a Raft node whose state machine is an OrderBook.
 *
 * <p>The leader acts as the sequencer: it takes client orders, stamps a
 * timestamp, and {@link #flush() proposes} them to Raft as one batch (group
 * commit). Every node applies committed commands to its own book in log
 * order, so all books go through the same states.
 *
 * <p>A client is acknowledged only when its order is <b>applied</b> on the
 * leader, which means it is committed on a majority and survives any single
 * failure. An order that was never acked may or may not have made it; the
 * client resends it with the same {@code clOrdId}, and duplicates are
 * dropped when applied, so a resend never creates a second order.
 */
public final class ExchangeNode {

    /** Told when an order this node proposed has been applied (committed). */
    @FunctionalInterface
    public interface AckListener {
        void onAck(int clientId, long clOrdId);
    }

    /**
     * Brackets every committed command as it's applied, on every node. Book
     * events fired in between belong to this command, which lets a gateway
     * know which client an order id belongs to. Deterministic like the book:
     * every node sees the same calls in the same order.
     */
    public interface ApplyHooks {
        void before(int clientId, long clOrdId, boolean duplicate);
        void after();

        /** The whole state was just replaced from a snapshot: rebuild anything derived. */
        default void restored() { }

        ApplyHooks NONE = new ApplyHooks() {
            @Override public void before(int clientId, long clOrdId, boolean duplicate) { }
            @Override public void after() { }
        };
    }

    private record ClientOrder(int clientId, long clOrdId) { }

    /**
     * Which clOrdIds a client already had applied: every id up to
     * {@code contiguous}, plus the few above it. Clients number orders
     * 1, 2, 3..., so this stays tiny however long the exchange runs, and it
     * is exact: an id counts as a duplicate only if it really was applied.
     */
    private static final class Seen {
        long contiguous;
        final TreeSet<Long> above = new TreeSet<>();

        /** True if new (and records it), false if already applied. */
        boolean add(long clOrdId) {
            if (clOrdId <= contiguous || !above.add(clOrdId)) {
                return false;
            }
            while (!above.isEmpty() && above.first() == contiguous + 1) {
                contiguous = above.pollFirst();
            }
            return true;
        }
    }

    private final RaftNode raft;
    private final BookFactory books;
    private final ExecutionListener reports;
    private OrderBook book;
    private final AckListener acks;

    private final List<byte[]> batch = new ArrayList<>();
    private final List<ClientOrder> batchKeys = new ArrayList<>();
    private final TreeMap<Long, ClientOrder> awaitingAck = new TreeMap<>();  // log index → order
    // Part of the replicated state: all nodes apply the same log, so all make
    // the same duplicate decisions; it's in every snapshot too.
    private final Map<Integer, Seen> seen = new HashMap<>();
    private long duplicatesDropped;
    private ApplyHooks hooks = ApplyHooks.NONE;

    public ExchangeNode(int id, int clusterSize, Storage storage, Transport transport,
                        Random random, RaftNode.Config config,
                        BookFactory books, ExecutionListener reports, AckListener acks) {
        this.books = books;
        this.reports = reports;
        this.book = books.create(reports);
        this.acks = acks;
        StateMachine sm = new StateMachine() {
            @Override public void apply(long index, byte[] command) { ExchangeNode.this.apply(index, command); }
            @Override public byte[] snapshot() { return ExchangeNode.this.snapshot(); }
            @Override public void restore(byte[] data) { ExchangeNode.this.restore(data); }
        };
        this.raft = new RaftNode(id, clusterSize, storage, transport, sm, random, config);
    }

    public void setApplyHooks(ApplyHooks hooks) {
        this.hooks = hooks;
    }

    // --- client side (leader only) ---

    /** Queues a new order for the next batch. False if this node isn't the leader. */
    public boolean submitNew(int clientId, long clOrdId, Side side, OrderType type,
                             long price, long qty, long timestamp) {
        return queue(OrderCommand.newOrder(clientId, clOrdId, side, type, price, qty, timestamp),
                clientId, clOrdId);
    }

    public boolean submitCancel(int clientId, long clOrdId, long orderId) {
        return queue(OrderCommand.cancel(clientId, clOrdId, orderId), clientId, clOrdId);
    }

    private boolean queue(byte[] command, int clientId, long clOrdId) {
        if (raft.role() != Role.LEADER) {
            return false;
        }
        batch.add(command);
        batchKeys.add(new ClientOrder(clientId, clOrdId));
        return true;
    }

    /** Proposes everything queued as one Raft batch: one fsync, one round trip. */
    public void flush() {
        if (batch.isEmpty()) {
            return;
        }
        long first = raft.proposeAll(batch);
        if (first != -1) {
            for (int i = 0; i < batchKeys.size(); i++) {
                awaitingAck.put(first + i, batchKeys.get(i));
            }
        }
        batch.clear();
        batchKeys.clear();
    }

    // --- Raft plumbing ---

    public void tick() {
        raft.tick();
    }

    public void handle(Message m) {
        raft.handle(m);
    }

    /** Raft's state machine: every committed command, in log order, on every node. */
    private void apply(long index, byte[] command) {
        ClientOrder key = new ClientOrder(OrderCommand.clientId(command), OrderCommand.clOrdId(command));
        boolean fresh = seen.computeIfAbsent(key.clientId(), c -> new Seen()).add(key.clOrdId());
        hooks.before(key.clientId(), key.clOrdId(), !fresh);
        if (fresh) {
            OrderCommand.applyTo(command, book);
        } else {
            duplicatesDropped++;                // a resend of something already applied
        }
        // Ack only if this index still holds what we proposed there; after a
        // leader change it may have been replaced by someone else's command.
        ClientOrder proposed = awaitingAck.remove(index);
        if (proposed != null && proposed.equals(key)) {
            acks.onAck(key.clientId(), key.clOrdId());
        }
        // Anything we proposed at an earlier index is settled by now (no-op
        // entries never reach apply), so stop waiting for it.
        awaitingAck.headMap(index).clear();
        hooks.after();
    }

    // --- snapshots ---

    private static final int SNAPSHOT_VERSION = 1;

    /** Everything replaying the log so far produced: resting orders, counters, dedup state. */
    private byte[] snapshot() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(SNAPSHOT_VERSION);
            out.writeLong(book.nextOrderId());
            out.writeLong(book.lastTradePrice());
            out.writeInt(book.liveOrders());
            IOException[] failed = new IOException[1];
            book.forEachResting(o -> {
                try {
                    out.writeLong(o.orderId());
                    out.writeInt(o.clientId());
                    out.writeLong(o.clOrdId());
                    out.writeByte(o.side().ordinal());
                    out.writeByte(o.type().ordinal());
                    out.writeLong(o.price());
                    out.writeLong(o.qty());
                    out.writeLong(o.leaves());
                    out.writeLong(o.timestamp());
                } catch (IOException e) {
                    failed[0] = e;
                }
            });
            if (failed[0] != null) {
                throw failed[0];
            }
            out.writeInt(seen.size());
            for (Map.Entry<Integer, Seen> e : seen.entrySet()) {
                out.writeInt(e.getKey());
                out.writeLong(e.getValue().contiguous);
                out.writeInt(e.getValue().above.size());
                for (long id : e.getValue().above) {
                    out.writeLong(id);
                }
            }
            out.writeLong(duplicatesDropped);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Replaces all state with a snapshot; the book is rebuilt from scratch. */
    private void restore(byte[] data) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            if (in.readInt() != SNAPSHOT_VERSION) {
                throw new IOException("unknown snapshot version");
            }
            long nextOrderId = in.readLong();
            long lastTrade = in.readLong();
            book = books.create(reports);
            int orders = in.readInt();
            for (int i = 0; i < orders; i++) {
                book.restoreResting(in.readLong(), in.readInt(), in.readLong(),
                        Side.values()[in.readByte()], OrderType.values()[in.readByte()],
                        in.readLong(), in.readLong(), in.readLong(), in.readLong());
            }
            book.restoreCounters(nextOrderId, lastTrade);
            seen.clear();
            int clients = in.readInt();
            for (int i = 0; i < clients; i++) {
                Seen s = new Seen();
                int clientId = in.readInt();
                s.contiguous = in.readLong();
                int above = in.readInt();
                for (int k = 0; k < above; k++) {
                    s.above.add(in.readLong());
                }
                seen.put(clientId, s);
            }
            duplicatesDropped = in.readLong();
            awaitingAck.clear();                // those log positions are behind us now
            hooks.restored();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // --- views ---

    public Role role() { return raft.role(); }
    public long term() { return raft.term(); }
    /** The leader this node believes in, or -1. */
    public int leaderId() { return raft.leaderId(); }
    public long commitIndex() { return raft.commitIndex(); }
    public long lastIndex() { return raft.lastIndex(); }
    public long lastApplied() { return raft.lastApplied(); }
    public OrderBook book() { return book; }
    public long duplicatesDropped() { return duplicatesDropped; }
}
