package com.tradeengine.bench;

import com.tradeengine.orderbook.ExecutionListener;
import com.tradeengine.orderbook.RejectReason;

/**
 * Cheapest listener that still does real work: it counts events, so the
 * JIT cannot decide the book's output is unused and delete the matching.
 */
final class CountingListener implements ExecutionListener {

    long accepted;
    long trades;
    long tradedQty;
    long cancelled;
    long rejected;

    @Override
    public void onAccepted(long orderId, long clOrdId) {
        accepted++;
    }

    @Override
    public void onTrade(long makerOrderId, long takerOrderId, long price, long qty) {
        trades++;
        tradedQty += qty;
    }

    @Override
    public void onCancelled(long orderId, long leaves) {
        cancelled++;
    }

    @Override
    public void onRejected(long clOrdId, RejectReason reason) {
        rejected++;
    }
}
