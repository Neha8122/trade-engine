package com.tradeengine.orderbook;

public enum OrderType {
    /** Match what crosses, rest the remainder in the book. */
    LIMIT,
    /** Match at any price, cancel whatever is left. Never rests. */
    MARKET,
    /** Immediate-or-cancel: match up to the limit price, cancel the rest. */
    IOC
}
