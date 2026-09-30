package com.tradeengine.exchange;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.tradeengine.orderbook.ExecutionListener;
import com.tradeengine.orderbook.OrderBook;
import com.tradeengine.orderbook.OrderType;
import com.tradeengine.orderbook.RejectReason;
import com.tradeengine.orderbook.Side;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class OrderCommandTest {

    private final List<String> out = new ArrayList<>();
    private final OrderBook book = new OrderBook(1, 100_000, 200, 16, new ExecutionListener() {
        @Override public void onAccepted(long orderId, long clOrdId) { out.add("A" + orderId + "/" + clOrdId); }
        @Override public void onTrade(long m, long t, long p, long q) { out.add("T" + m + "/" + t + " " + q + "@" + p); }
        @Override public void onCancelled(long orderId, long leaves) { out.add("C" + orderId + " " + leaves); }
        @Override public void onRejected(long clOrdId, RejectReason r) { out.add("R" + clOrdId + " " + r); }
    });

    @Test
    void newOrderRoundTrip() {
        byte[] sell = OrderCommand.newOrder(7, 11, Side.SELL, OrderType.LIMIT, 100_050, 30, 999);
        byte[] buy = OrderCommand.newOrder(8, 12, Side.BUY, OrderType.IOC, 100_050, 50, 1000);
        assertEquals(7, OrderCommand.clientId(sell));
        assertEquals(11, OrderCommand.clOrdId(sell));

        OrderCommand.applyTo(sell, book);
        OrderCommand.applyTo(buy, book);
        assertEquals(List.of("A1/11", "A2/12", "T1/2 30@100050", "C2 20"), out);
    }

    @Test
    void cancelRoundTrip() {
        OrderCommand.applyTo(OrderCommand.newOrder(7, 11, Side.BUY, OrderType.LIMIT, 100_040, 5, 0), book);
        OrderCommand.applyTo(OrderCommand.cancel(7, 12, 1), book);
        OrderCommand.applyTo(OrderCommand.cancel(7, 13, 1), book);
        assertEquals(List.of("A1/11", "C1 5", "R13 UNKNOWN_ORDER"), out);
    }
}
