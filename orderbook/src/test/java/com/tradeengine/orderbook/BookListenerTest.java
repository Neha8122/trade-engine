package com.tradeengine.orderbook;

import static com.tradeengine.orderbook.OrderType.IOC;
import static com.tradeengine.orderbook.OrderType.LIMIT;
import static com.tradeengine.orderbook.OrderType.MARKET;
import static com.tradeengine.orderbook.Side.BUY;
import static com.tradeengine.orderbook.Side.SELL;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The public events: only resting orders appear, never client ids. */
class BookListenerTest {

    private final List<String> pub = new ArrayList<>();
    private final OrderBook book = new OrderBook(1, 100_000, 200, 16,
            new RecordingListener(), new BookListener() {
                @Override
                public void onAdd(long orderId, Side side, long price, long qty) {
                    pub.add("ADD #" + orderId + " " + side + " " + qty + " @" + price);
                }

                @Override
                public void onExecute(long orderId, long qty, long price) {
                    pub.add("EXEC #" + orderId + " " + qty + " @" + price);
                }

                @Override
                public void onDelete(long orderId) {
                    pub.add("DEL #" + orderId);
                }
            });

    @Test
    void restingOrderIsAdded() {
        long id = book.newOrder(1, 1, BUY, LIMIT, 100_040, 50, 0);
        assertEquals(List.of("ADD #" + id + " BUY 50 @100040"), pub);
    }

    @Test
    void fullyFilledTakerIsNeverAdded() {
        long sell = book.newOrder(1, 1, SELL, LIMIT, 100_050, 50, 0);
        book.newOrder(1, 2, BUY, LIMIT, 100_050, 50, 0);

        assertEquals(List.of(
                "ADD #" + sell + " SELL 50 @100050",
                "EXEC #" + sell + " 50 @100050"), pub);
    }

    @Test
    void partlyFilledTakerIsAddedWithWhatIsLeft() {
        long sell = book.newOrder(1, 1, SELL, LIMIT, 100_050, 30, 0);
        long buy = book.newOrder(1, 2, BUY, LIMIT, 100_050, 100, 0);

        assertEquals(List.of(
                "ADD #" + sell + " SELL 30 @100050",
                "EXEC #" + sell + " 30 @100050",
                "ADD #" + buy + " BUY 70 @100050"), pub);
    }

    @Test
    void iocAndMarketLeftoversProduceNoPublicEvent() {
        book.newOrder(1, 1, BUY, IOC, 100_050, 10, 0);
        book.newOrder(1, 2, SELL, MARKET, 0, 10, 0);
        assertEquals(List.of(), pub);
    }

    @Test
    void cancelIsADelete() {
        long id = book.newOrder(1, 1, SELL, LIMIT, 100_060, 10, 0);
        book.cancel(2, id);
        book.cancel(3, id);             // rejected: no second delete

        assertEquals(List.of("ADD #" + id + " SELL 10 @100060", "DEL #" + id), pub);
    }
}
