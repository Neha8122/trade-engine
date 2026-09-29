package com.tradeengine.orderbook;

import java.util.ArrayList;
import java.util.List;

/** Test listener: turns every event into a readable string, in order. */
final class RecordingListener implements ExecutionListener {

    final List<String> events = new ArrayList<>();

    @Override
    public void onAccepted(long orderId, long clOrdId) {
        events.add("ACCEPTED #" + orderId);
    }

    @Override
    public void onTrade(long makerOrderId, long takerOrderId, long price, long qty) {
        events.add("TRADE maker=#" + makerOrderId + " taker=#" + takerOrderId
                + " " + qty + " @" + price);
    }

    @Override
    public void onCancelled(long orderId, long leaves) {
        events.add("CANCELLED #" + orderId + " leaves=" + leaves);
    }

    @Override
    public void onRejected(long clOrdId, RejectReason reason) {
        events.add("REJECTED clOrdId=" + clOrdId + " " + reason);
    }

    /** Returns what happened since the last call, then forgets it. */
    List<String> drain() {
        List<String> out = List.copyOf(events);
        events.clear();
        return out;
    }
}
