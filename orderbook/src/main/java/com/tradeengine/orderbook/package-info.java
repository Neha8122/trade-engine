/**
 * Single-threaded, price-time priority order book.
 *
 * <p>Design: docs/lld-orderbook.html. One thread owns a book, so nothing
 * in this package takes a lock, and nothing on the hot path allocates.
 */
package com.tradeengine.orderbook;
