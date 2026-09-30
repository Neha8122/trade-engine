package com.tradeengine.exchange;

import com.tradeengine.orderbook.ExecutionListener;
import com.tradeengine.orderbook.OrderBook;
import com.tradeengine.orderbook.OrderType;
import com.tradeengine.orderbook.Side;
import com.tradeengine.raft.Message;
import com.tradeengine.raft.RaftNode;
import com.tradeengine.raft.Role;
import com.tradeengine.raft.Storage;
import com.tradeengine.raft.Transport;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

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

        ApplyHooks NONE = new ApplyHooks() {
            @Override public void before(int clientId, long clOrdId, boolean duplicate) { }
            @Override public void after() { }
        };
    }

    private record ClientOrder(int clientId, long clOrdId) { }

    private final RaftNode raft;
    private final OrderBook book;
    private final AckListener acks;

    private final List<byte[]> batch = new ArrayList<>();
    private final List<ClientOrder> batchKeys = new ArrayList<>();
    private final TreeMap<Long, ClientOrder> awaitingAck = new TreeMap<>();  // log index → order
    // Every (clientId, clOrdId) ever applied. Part of the replicated state:
    // all nodes apply the same log, so all make the same duplicate decisions.
    private final Set<ClientOrder> seen = new HashSet<>();
    private long duplicatesDropped;
    private ApplyHooks hooks = ApplyHooks.NONE;

    public ExchangeNode(int id, int clusterSize, Storage storage, Transport transport,
                        Random random, RaftNode.Config config,
                        BookFactory books, ExecutionListener reports, AckListener acks) {
        this.book = books.create(reports);
        this.acks = acks;
        this.raft = new RaftNode(id, clusterSize, storage, transport, this::apply, random, config);
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
        boolean fresh = seen.add(key);
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
