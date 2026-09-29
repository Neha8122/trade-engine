package com.tradeengine.feed;

import com.tradeengine.orderbook.BookListener;
import com.tradeengine.orderbook.Side;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * A subscriber's copy of the book, rebuilt order by order from the feed.
 *
 * <p>Written for clarity, not speed: subscribers are outside the
 * exchange's hot path, and tests compare it level by level with the real
 * {@link com.tradeengine.orderbook.OrderBook}.
 */
public final class BookReplica implements BookListener {

    private record Resting(Side side, long price, long qty) { }

    private final Map<Long, Resting> orders = new HashMap<>();
    private final TreeMap<Long, Long> bids = new TreeMap<>(Comparator.reverseOrder());
    private final TreeMap<Long, Long> asks = new TreeMap<>();

    @Override
    public void onAdd(long orderId, Side side, long price, long qty) {
        orders.put(orderId, new Resting(side, price, qty));
        side(side).merge(price, qty, Long::sum);
    }

    @Override
    public void onExecute(long orderId, long qty, long price) {
        Resting o = orders.get(orderId);
        if (o == null) {
            return;     // only possible after unrecovered loss
        }
        long left = o.qty - qty;
        if (left == 0) {
            orders.remove(orderId);
        } else {
            orders.put(orderId, new Resting(o.side, o.price, left));
        }
        reduce(o.side, o.price, qty);
    }

    @Override
    public void onDelete(long orderId) {
        Resting o = orders.remove(orderId);
        if (o != null) {
            reduce(o.side, o.price, o.qty);
        }
    }

    private void reduce(Side side, long price, long qty) {
        TreeMap<Long, Long> levels = side(side);
        long left = levels.get(price) - qty;
        if (left == 0) {
            levels.remove(price);
        } else {
            levels.put(price, left);
        }
    }

    private TreeMap<Long, Long> side(Side side) {
        return side == Side.BUY ? bids : asks;
    }

    /** Total resting qty at a price, 0 if none. */
    public long levelQty(Side side, long price) {
        return side(side).getOrDefault(price, 0L);
    }

    public Long bestBid() { return bids.isEmpty() ? null : bids.firstKey(); }
    public Long bestAsk() { return asks.isEmpty() ? null : asks.firstKey(); }
    public int orderCount() { return orders.size(); }
}
