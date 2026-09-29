package com.tradeengine.orderbook;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * The slow, obviously-correct order book the fast {@link OrderBook} is
 * checked against. Written for readability only: TreeMap per side,
 * ArrayDeque per price, HashMap for ids. No pooling, no arrays, no tricks.
 *
 * <p>It follows the same rules and reports through the same
 * {@link ExecutionListener}, so both books' event streams can be compared
 * line by line.
 */
final class ReferenceBook {

    private static final class RefOrder {
        final long id;
        final Side side;
        final long price;
        long leaves;

        RefOrder(long id, Side side, long price, long leaves) {
            this.id = id;
            this.side = side;
            this.price = price;
            this.leaves = leaves;
        }
    }

    // Best price first on each side.
    private final TreeMap<Long, ArrayDeque<RefOrder>> bids = new TreeMap<>(Comparator.reverseOrder());
    private final TreeMap<Long, ArrayDeque<RefOrder>> asks = new TreeMap<>();
    private final Map<Long, RefOrder> live = new HashMap<>();

    private final long basePrice;
    private final int levels;
    private final int maxOrders;
    private final ExecutionListener listener;
    private long nextId = 1;

    ReferenceBook(long basePrice, int levels, int maxOrders, ExecutionListener listener) {
        this.basePrice = basePrice;
        this.levels = levels;
        this.maxOrders = maxOrders;
        this.listener = listener;
    }

    long newOrder(long clOrdId, Side side, OrderType type, long price, long qty) {
        if (qty <= 0) {
            listener.onRejected(clOrdId, RejectReason.BAD_QTY);
            return 0;
        }
        if (type != OrderType.MARKET && (price < basePrice || price >= basePrice + levels)) {
            listener.onRejected(clOrdId, RejectReason.OUT_OF_BAND);
            return 0;
        }
        if (live.size() == maxOrders) {
            listener.onRejected(clOrdId, RejectReason.POOL_FULL);
            return 0;
        }

        long id = nextId++;
        listener.onAccepted(id, clOrdId);
        long leaves = qty;

        TreeMap<Long, ArrayDeque<RefOrder>> other = side == Side.BUY ? asks : bids;
        while (leaves > 0 && !other.isEmpty()) {
            Map.Entry<Long, ArrayDeque<RefOrder>> best = other.firstEntry();
            long px = best.getKey();
            boolean crosses = type == OrderType.MARKET
                    || (side == Side.BUY ? px <= price : px >= price);
            if (!crosses) {
                break;
            }
            ArrayDeque<RefOrder> queue = best.getValue();
            RefOrder maker = queue.peekFirst();
            long fill = Math.min(leaves, maker.leaves);
            leaves -= fill;
            maker.leaves -= fill;
            listener.onTrade(maker.id, id, px, fill);
            if (maker.leaves == 0) {
                queue.pollFirst();
                live.remove(maker.id);
                if (queue.isEmpty()) {
                    other.remove(px);
                }
            }
        }

        if (leaves > 0) {
            if (type == OrderType.LIMIT) {
                RefOrder o = new RefOrder(id, side, price, leaves);
                (side == Side.BUY ? bids : asks)
                        .computeIfAbsent(price, k -> new ArrayDeque<>())
                        .addLast(o);
                live.put(id, o);
            } else {
                listener.onCancelled(id, leaves);
            }
        }
        return id;
    }

    boolean cancel(long clOrdId, long orderId) {
        RefOrder o = live.remove(orderId);
        if (o == null) {
            listener.onRejected(clOrdId, RejectReason.UNKNOWN_ORDER);
            return false;
        }
        TreeMap<Long, ArrayDeque<RefOrder>> side = o.side == Side.BUY ? bids : asks;
        ArrayDeque<RefOrder> queue = side.get(o.price);
        queue.remove(o);
        if (queue.isEmpty()) {
            side.remove(o.price);
        }
        listener.onCancelled(orderId, o.leaves);
        return true;
    }

    /** Best bid price, or {@code null} if no bids. */
    Long bestBid() { return bids.isEmpty() ? null : bids.firstKey(); }

    /** Best ask price, or {@code null} if no asks. */
    Long bestAsk() { return asks.isEmpty() ? null : asks.firstKey(); }

    int liveOrders() { return live.size(); }
}
