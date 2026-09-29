package com.tradeengine.orderbook;

import static com.tradeengine.orderbook.OrderType.IOC;
import static com.tradeengine.orderbook.OrderType.LIMIT;
import static com.tradeengine.orderbook.OrderType.MARKET;
import static com.tradeengine.orderbook.Side.BUY;
import static com.tradeengine.orderbook.Side.SELL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * One test per edge case in docs/lld-orderbook.html, section 5.
 * Prices are ticks: 100050 = 1000.50. The band is 1000.00 to 1001.99.
 */
class OrderBookTest {

    private static final long BASE = 100_000;
    private static final int LEVELS = 200;
    private static final int MAX_ORDERS = 16;

    private RecordingListener events;
    private OrderBook book;
    private long clOrdId;

    @BeforeEach
    void setUp() {
        events = new RecordingListener();
        book = new OrderBook(1, BASE, LEVELS, MAX_ORDERS, events);
    }

    // --- rejects: nothing in the book may change ---

    @Test
    void rejectsZeroOrNegativeQty() {
        assertEquals(0, send(BUY, LIMIT, 100050, 0));
        assertEquals(0, send(SELL, LIMIT, 100050, -5));

        assertEquals(List.of(
                "REJECTED clOrdId=1 BAD_QTY",
                "REJECTED clOrdId=2 BAD_QTY"), events.drain());
        assertEmptyBook();
    }

    @Test
    void rejectsLimitAndIocPricesOutsideBand() {
        send(BUY, LIMIT, BASE - 1, 10);             // just below band
        send(SELL, LIMIT, BASE + LEVELS, 10);       // just above band
        send(BUY, IOC, BASE + LEVELS + 50, 10);

        assertEquals(List.of(
                "REJECTED clOrdId=1 OUT_OF_BAND",
                "REJECTED clOrdId=2 OUT_OF_BAND",
                "REJECTED clOrdId=3 OUT_OF_BAND"), events.drain());
        assertEmptyBook();
    }

    @Test
    void acceptsBothEndsOfBand() {
        long low = send(BUY, LIMIT, BASE, 10);
        long high = send(SELL, LIMIT, BASE + LEVELS - 1, 10);

        assertEquals(BASE, book.bestBid().price());
        assertEquals(BASE + LEVELS - 1, book.bestAsk().price());
        assertEquals(List.of("ACCEPTED #" + low, "ACCEPTED #" + high), events.drain());
    }

    @Test
    void rejectsWhenPoolIsFull() {
        for (int i = 0; i < MAX_ORDERS; i++) {
            send(BUY, LIMIT, 100010, 1);
        }
        events.drain();

        assertEquals(0, send(BUY, LIMIT, 100010, 1));
        assertEquals(List.of("REJECTED clOrdId=" + clOrdId + " POOL_FULL"), events.drain());
        assertEquals(MAX_ORDERS, book.liveOrders());
    }

    // --- order types ---

    @Test
    void limitThatDoesNotCrossRests() {
        send(SELL, LIMIT, 100050, 100);
        long buy = send(BUY, LIMIT, 100040, 30);    // below best ask

        assertEquals(List.of("ACCEPTED #1", "ACCEPTED #" + buy), events.drain());
        assertEquals(100040, book.bestBid().price());
        assertEquals(100050, book.bestAsk().price());
        assertEquals(30, book.order(buy).leaves());
    }

    @Test
    void tradePriceIsTheMakersPrice() {
        long sell = send(SELL, LIMIT, 100050, 100);
        long buy = send(BUY, LIMIT, 100055, 100);   // willing to pay more

        assertEquals(List.of(
                "ACCEPTED #" + sell,
                "ACCEPTED #" + buy,
                "TRADE maker=#" + sell + " taker=#" + buy + " 100 @100050"), events.drain());
        assertEmptyBook();
    }

    @Test
    void limitPartlyFilledRestsTheRemainder() {
        long sell = send(SELL, LIMIT, 100050, 40);
        long buy = send(BUY, LIMIT, 100050, 100);

        assertEquals(List.of(
                "ACCEPTED #" + sell,
                "ACCEPTED #" + buy,
                "TRADE maker=#" + sell + " taker=#" + buy + " 40 @100050"), events.drain());
        assertNull(book.bestAsk());
        assertEquals(100050, book.bestBid().price());
        assertEquals(60, book.order(buy).leaves());
    }

    @Test
    void marketOrderAgainstEmptySideIsCancelled() {
        long buy = send(BUY, MARKET, 0, 100);

        assertEquals(List.of(
                "ACCEPTED #" + buy,
                "CANCELLED #" + buy + " leaves=100"), events.drain());
        assertEmptyBook();
    }

    @Test
    void marketOrderIgnoresPriceAndCancelsLeftover() {
        long a = send(SELL, LIMIT, 100050, 30);
        long b = send(SELL, LIMIT, 100150, 30);     // far away, still taken
        long buy = send(BUY, MARKET, 0, 100);

        assertEquals(List.of(
                "ACCEPTED #" + a,
                "ACCEPTED #" + b,
                "ACCEPTED #" + buy,
                "TRADE maker=#" + a + " taker=#" + buy + " 30 @100050",
                "TRADE maker=#" + b + " taker=#" + buy + " 30 @100150",
                "CANCELLED #" + buy + " leaves=40"), events.drain());
        assertEmptyBook();
    }

    @Test
    void iocFillsWhatItCanAndCancelsTheRest() {
        long a = send(SELL, LIMIT, 100050, 100);
        long b = send(SELL, LIMIT, 100051, 100);    // above the IOC limit
        long ioc = send(BUY, IOC, 100050, 150);

        assertEquals(List.of(
                "ACCEPTED #" + a,
                "ACCEPTED #" + b,
                "ACCEPTED #" + ioc,
                "TRADE maker=#" + a + " taker=#" + ioc + " 100 @100050",
                "CANCELLED #" + ioc + " leaves=50"), events.drain());
        assertNull(book.bestBid());                 // IOC never rests
        assertEquals(100051, book.bestAsk().price());
    }

    @Test
    void sellSideMatchesHighestBidFirst() {
        long low = send(BUY, LIMIT, 100040, 10);
        long high = send(BUY, LIMIT, 100045, 10);
        long sell = send(SELL, LIMIT, 100040, 15);

        assertEquals(List.of(
                "ACCEPTED #" + low,
                "ACCEPTED #" + high,
                "ACCEPTED #" + sell,
                "TRADE maker=#" + high + " taker=#" + sell + " 10 @100045",
                "TRADE maker=#" + low + " taker=#" + sell + " 5 @100040"), events.drain());
        assertEquals(5, book.order(low).leaves());
    }

    // --- priority ---

    @Test
    void samePriceOlderOrderFillsFirst() {
        long first = send(SELL, LIMIT, 100050, 100);
        long second = send(SELL, LIMIT, 100050, 100);
        long buy = send(BUY, LIMIT, 100050, 150);

        assertEquals(List.of(
                "ACCEPTED #" + first,
                "ACCEPTED #" + second,
                "ACCEPTED #" + buy,
                "TRADE maker=#" + first + " taker=#" + buy + " 100 @100050",
                "TRADE maker=#" + second + " taker=#" + buy + " 50 @100050"), events.drain());
        PriceLevel level = book.bestAsk();
        assertSame(book.order(second), level.head());
        assertEquals(50, level.totalQty());
    }

    @Test
    void oneOrderSweepsSeveralLevelsAndBestMoves() {
        send(SELL, LIMIT, 100050, 10);
        send(SELL, LIMIT, 100052, 10);
        send(SELL, LIMIT, 100055, 10);
        send(SELL, LIMIT, 100060, 10);

        send(BUY, LIMIT, 100055, 25);   // takes 1000.50, 1000.52, half of 1000.55

        assertEquals(100055, book.bestAsk().price());
        assertEquals(5, book.bestAsk().totalQty());
        assertTrue(book.level(SELL, 100050).isEmpty());
        assertTrue(book.level(SELL, 100052).isEmpty());
        assertNull(book.bestBid());     // taker was fully filled
    }

    // --- cancel ---

    @Test
    void cancelRemovesRestingOrder() {
        long id = send(BUY, LIMIT, 100040, 70);
        events.drain();

        assertTrue(book.cancel(99, id));
        assertEquals(List.of("CANCELLED #" + id + " leaves=70"), events.drain());
        assertEmptyBook();
    }

    @Test
    void cancelUnknownOrderIsRejected() {
        assertFalse(book.cancel(7, 12345));
        assertEquals(List.of("REJECTED clOrdId=7 UNKNOWN_ORDER"), events.drain());
    }

    @Test
    void cancelAlreadyFilledOrderIsRejected() {
        long sell = send(SELL, LIMIT, 100050, 10);
        send(BUY, LIMIT, 100050, 10);
        events.drain();

        assertFalse(book.cancel(8, sell));
        assertEquals(List.of("REJECTED clOrdId=8 UNKNOWN_ORDER"), events.drain());
    }

    @Test
    void cancelTwiceIsRejectedTheSecondTime() {
        long id = send(SELL, LIMIT, 100050, 10);
        book.cancel(1, id);
        events.drain();

        assertFalse(book.cancel(2, id));
        assertEquals(List.of("REJECTED clOrdId=2 UNKNOWN_ORDER"), events.drain());
    }

    @Test
    void cancellingLastOrderAtBestMovesBest() {
        long top = send(BUY, LIMIT, 100045, 10);
        long next = send(BUY, LIMIT, 100030, 10);

        book.cancel(1, top);
        assertEquals(100030, book.bestBid().price());

        book.cancel(2, next);
        assertNull(book.bestBid());
    }

    @Test
    void cancellingBelowBestLeavesBestAlone() {
        send(SELL, LIMIT, 100050, 10);
        long worse = send(SELL, LIMIT, 100070, 10);

        book.cancel(1, worse);
        assertEquals(100050, book.bestAsk().price());
    }

    @Test
    void cancelInMiddleOfQueueKeepsOthersInOrder() {
        long a = send(SELL, LIMIT, 100050, 10);
        long b = send(SELL, LIMIT, 100050, 20);
        long c = send(SELL, LIMIT, 100050, 30);
        book.cancel(1, b);
        events.drain();

        long buy = send(BUY, LIMIT, 100050, 40);
        assertEquals(List.of(
                "ACCEPTED #" + buy,
                "TRADE maker=#" + a + " taker=#" + buy + " 10 @100050",
                "TRADE maker=#" + c + " taker=#" + buy + " 30 @100050"), events.drain());
    }

    // --- memory: nothing leaks ---

    @Test
    void everyOrderGoesBackToThePool() {
        long a = send(SELL, LIMIT, 100050, 10);
        send(SELL, LIMIT, 100051, 10);
        send(BUY, LIMIT, 100051, 15);   // fills a, half of the second
        send(BUY, IOC, 100040, 5);      // nothing to hit, cancelled
        send(BUY, MARKET, 0, 50);       // takes the rest, leftover cancelled
        book.cancel(1, a);              // already gone: rejected

        assertEmptyBook();
    }

    // --- helpers ---

    private long send(Side side, OrderType type, long price, long qty) {
        return book.newOrder(7, ++clOrdId, side, type, price, qty, clOrdId);
    }

    private void assertEmptyBook() {
        assertNull(book.bestBid(), "bestBid");
        assertNull(book.bestAsk(), "bestAsk");
        assertEquals(0, book.liveOrders(), "live orders");
        assertEquals(MAX_ORDERS, book.poolAvailable(), "orders missing from pool");
    }
}
