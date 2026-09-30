package com.tradeengine.exchange;

import com.tradeengine.orderbook.ExecutionListener;
import com.tradeengine.orderbook.OrderBook;

/** Builds a fresh, empty book: at startup, and again after every restart. */
@FunctionalInterface
public interface BookFactory {

    OrderBook create(ExecutionListener reports);
}
