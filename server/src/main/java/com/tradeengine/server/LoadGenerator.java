package com.tradeengine.server;

import com.tradeengine.orderbook.OrderType;
import com.tradeengine.orderbook.Side;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;

/**
 * Sends orders at a fixed rate and measures order → ack latency with an
 * HdrHistogram.
 *
 * <p>Open loop: order k is <i>due</i> at {@code start + k / rate}, whether or
 * not earlier orders were acked, and its latency is measured from that due
 * time. If the exchange stalls for 300 ms, the orders due during the stall
 * are recorded as up to 300 ms late. A closed-loop tester that waits for
 * each ack before sending the next would record one slow order and hide
 * the rest: that is coordinated omission, and it makes p99 look far better
 * than what clients actually saw.
 *
 * <p>Orders alternate: a SELL of 1 that rests, then a BUY IOC of 1 that
 * takes it, so the book stays small however long the run is.
 */
final class LoadGenerator {

    private static final long PRICE = 101_000;

    private LoadGenerator() { }

    static void run(Map<String, String> o) throws Exception {
        InetSocketAddress[] nodes = Main.addresses(o.get("clients"));
        int rate = Integer.parseInt(o.getOrDefault("rate", "10000"));
        int seconds = Integer.parseInt(o.getOrDefault("seconds", "20"));
        int warmup = Integer.parseInt(o.getOrDefault("warmup", "5"));

        Histogram latency = new Histogram(TimeUnit.SECONDS.toNanos(60), 3);
        long[] acks = new long[1];
        long[] rejects = new long[1];
        long[] lastAck = {System.nanoTime()};
        long[] maxGap = new long[1];
        long measureFrom = System.nanoTime() + TimeUnit.SECONDS.toNanos(warmup);

        ExchangeClient client = new ExchangeClient(nodes, 1, new ExchangeClient.Listener() {
            @Override
            public void onAck(long clOrdId, long orderId, long latencyNanos) {
                long now = System.nanoTime();
                acks[0]++;
                if (now >= measureFrom) {
                    latency.recordValue(Math.min(latencyNanos, latency.getHighestTrackableValue()));
                    maxGap[0] = Math.max(maxGap[0], now - lastAck[0]);
                }
                lastAck[0] = now;
            }

            @Override
            public void onReject(long clOrdId, int reason) {
                rejects[0]++;
            }
        });

        // Wait until connected to a leader that acks, before the clock starts.
        long clOrdId = 0;
        client.send(++clOrdId, Side.BUY, OrderType.IOC, PRICE, 1, System.nanoTime());
        while (acks[0] + rejects[0] == 0) {
            client.poll();
        }

        long interval = TimeUnit.SECONDS.toNanos(1) / rate;
        long start = System.nanoTime();
        long end = start + TimeUnit.SECONDS.toNanos(warmup + seconds);
        long due = start;
        long sent = 0;
        while (System.nanoTime() < end) {
            long now = System.nanoTime();
            while (due <= now) {                            // catch up on anything due
                boolean sell = (sent & 1) == 0;
                client.send(++clOrdId, sell ? Side.SELL : Side.BUY, sell ? OrderType.LIMIT : OrderType.IOC,
                        PRICE, 1, due);                     // latency measured from the due time
                due += interval;
                sent++;
            }
            client.poll();
        }
        long drainUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (client.unacked() > 0 && System.nanoTime() < drainUntil) {
            client.poll();
        }
        client.close();

        System.out.printf("rate %,d/s for %d s (after %d s warm-up): %,d orders sent, %,d acked, %,d rejected, "
                        + "%d unacked, %d reconnects%n",
                rate, seconds, warmup, sent, acks[0] - 1, rejects[0], client.unacked(), client.reconnects());
        System.out.printf("order -> ack latency (us):  p50 %.0f   p90 %.0f   p99 %.0f   p99.9 %.0f   max %.0f%n",
                us(latency.getValueAtPercentile(50)), us(latency.getValueAtPercentile(90)),
                us(latency.getValueAtPercentile(99)), us(latency.getValueAtPercentile(99.9)),
                us(latency.getMaxValue()));
        System.out.printf("longest gap between two acks: %.0f ms%n", maxGap[0] / 1e6);
    }

    private static double us(long nanos) {
        return nanos / 1_000.0;
    }
}
