package com.tradeengine.feed;

import com.tradeengine.orderbook.BookListener;
import com.tradeengine.orderbook.Side;

/**
 * Plugs into the order book as its {@link BookListener} and turns each
 * public event into a feed message. Runs on the book's thread; encoding
 * writes straight into the packet buffer, so it allocates nothing.
 */
public final class MarketDataPublisher implements BookListener {

    private final Packetizer packetizer;

    public MarketDataPublisher(Packetizer packetizer) {
        this.packetizer = packetizer;
    }

    @Override
    public void onAdd(long orderId, Side side, long price, long qty) {
        packetizer.add(orderId, side, price, qty);
    }

    @Override
    public void onExecute(long orderId, long qty, long price) {
        packetizer.execute(orderId, qty, price);
    }

    @Override
    public void onDelete(long orderId) {
        packetizer.delete(orderId);
    }

    /** Call when the engine finishes a batch of commands. */
    public void flush() {
        packetizer.flush();
    }
}
