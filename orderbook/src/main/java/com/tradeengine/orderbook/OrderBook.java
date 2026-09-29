package com.tradeengine.orderbook;

/**
 * Price-time priority order book for one symbol.
 *
 * <p>Single writer: exactly one thread calls this class, so it has no locks
 * and no volatile fields. Nothing here allocates after construction.
 *
 * <p>Layout (docs/lld-orderbook.html, diagram 2): prices live in a fixed
 * band {@code [basePrice, basePrice + levels)}. Each side is an array of
 * {@link PriceLevel} indexed by {@code price - basePrice}, so finding a
 * level is array access, not a tree search.
 */
public final class OrderBook {

    private static final int NONE = -1;

    private final int symbolId;
    private final long basePrice;
    private final int levels;
    private final PriceLevel[] bids;
    private final PriceLevel[] asks;
    private final OrderPool pool;
    private final LongObjectMap<Order> index;
    private final ExecutionListener listener;
    private final BookListener bookListener;

    private int bestBid = NONE;     // highest non-empty bid index
    private int bestAsk = NONE;     // lowest non-empty ask index
    private long nextOrderId = 1;   // deterministic: same input, same ids

    /**
     * @param basePrice lowest valid price, in ticks
     * @param levels    number of valid prices, starting at basePrice
     * @param maxOrders most orders live at once (sizes pool and index)
     */
    public OrderBook(int symbolId, long basePrice, int levels, int maxOrders,
                     ExecutionListener listener) {
        this(symbolId, basePrice, levels, maxOrders, listener, BookListener.NONE);
    }

    /**
     * @param bookListener receives the public events (market data): adds,
     *                     executions and deletes of resting orders
     */
    public OrderBook(int symbolId, long basePrice, int levels, int maxOrders,
                     ExecutionListener listener, BookListener bookListener) {
        if (levels <= 0) {
            throw new IllegalArgumentException("levels must be > 0");
        }
        this.symbolId = symbolId;
        this.basePrice = basePrice;
        this.levels = levels;
        this.listener = listener;
        this.bookListener = bookListener;
        this.pool = new OrderPool(maxOrders);
        this.index = new LongObjectMap<>(maxOrders);
        this.bids = new PriceLevel[levels];
        this.asks = new PriceLevel[levels];
        // Every level exists up front: resting an order never allocates.
        for (int i = 0; i < levels; i++) {
            bids[i] = new PriceLevel(basePrice + i);
            asks[i] = new PriceLevel(basePrice + i);
        }
    }

    /**
     * Matches a new order against the book, then rests or cancels what is
     * left depending on its type (docs/lld-orderbook.html, diagram 3).
     *
     * @param price limit price in ticks; ignored for MARKET orders
     * @return the new orderId, or 0 if the order was rejected
     */
    public long newOrder(int clientId, long clOrdId, Side side, OrderType type,
                         long price, long qty, long timestamp) {
        if (qty <= 0) {
            listener.onRejected(clOrdId, RejectReason.BAD_QTY);
            return 0;
        }
        if (type != OrderType.MARKET && !inBand(price)) {
            listener.onRejected(clOrdId, RejectReason.OUT_OF_BAND);
            return 0;
        }
        Order o = pool.acquire();
        if (o == null) {
            listener.onRejected(clOrdId, RejectReason.POOL_FULL);
            return 0;
        }

        o.orderId = nextOrderId++;
        o.clientId = clientId;
        o.clOrdId = clOrdId;
        o.side = side;
        o.type = type;
        o.price = price;
        o.qty = qty;
        o.leaves = qty;
        o.timestamp = timestamp;
        listener.onAccepted(o.orderId, clOrdId);

        if (side == Side.BUY) {
            matchBuy(o);
        } else {
            matchSell(o);
        }

        long orderId = o.orderId;
        if (o.leaves == 0) {
            pool.release(o);                    // fully filled
        } else if (type == OrderType.LIMIT) {
            rest(o);
        } else {
            listener.onCancelled(orderId, o.leaves);   // IOC / MARKET leftover
            pool.release(o);
        }
        return orderId;
    }

    /**
     * Cancels a resting order. O(1): the index finds the order, and the
     * order's own links let its level unlink it without searching.
     */
    public boolean cancel(long clOrdId, long orderId) {
        Order o = index.get(orderId);
        if (o == null) {
            // Unknown, or already filled / cancelled.
            listener.onRejected(clOrdId, RejectReason.UNKNOWN_ORDER);
            return false;
        }
        PriceLevel level = o.level;
        int i = indexOf(level.price);
        long leaves = o.leaves;

        level.remove(o);
        index.remove(orderId);
        if (level.isEmpty()) {
            if (o.side == Side.BUY && i == bestBid) {
                bestBid = nextBidAtOrBelow(i - 1);
            } else if (o.side == Side.SELL && i == bestAsk) {
                bestAsk = nextAskAtOrAbove(i + 1);
            }
        }
        pool.release(o);
        listener.onCancelled(orderId, leaves);
        bookListener.onDelete(orderId);
        return true;
    }

    // --- matching ---

    /** Buy takes asks from the lowest price up, while the price crosses. */
    private void matchBuy(Order taker) {
        while (taker.leaves > 0 && bestAsk != NONE) {
            PriceLevel level = asks[bestAsk];
            if (taker.type != OrderType.MARKET && level.price > taker.price) {
                return;                         // best ask too expensive
            }
            fillAgainst(level, taker);
            if (level.isEmpty()) {
                bestAsk = nextAskAtOrAbove(bestAsk + 1);
            }
        }
    }

    /** Sell takes bids from the highest price down, while the price crosses. */
    private void matchSell(Order taker) {
        while (taker.leaves > 0 && bestBid != NONE) {
            PriceLevel level = bids[bestBid];
            if (taker.type != OrderType.MARKET && level.price < taker.price) {
                return;                         // best bid too low
            }
            fillAgainst(level, taker);
            if (level.isEmpty()) {
                bestBid = nextBidAtOrBelow(bestBid - 1);
            }
        }
    }

    /** One fill against the oldest order at this level (time priority). */
    private void fillAgainst(PriceLevel level, Order taker) {
        Order maker = level.head;
        long fill = Math.min(taker.leaves, maker.leaves);
        taker.leaves -= fill;
        // The trade prints at the resting order's price, not the taker's.
        listener.onTrade(maker.orderId, taker.orderId, level.price, fill);
        bookListener.onExecute(maker.orderId, fill, level.price);

        if (fill == maker.leaves) {
            level.remove(maker);
            index.remove(maker.orderId);
            pool.release(maker);
        } else {
            level.reduce(maker, fill);
        }
    }

    // --- resting ---

    private void rest(Order o) {
        int i = indexOf(o.price);
        index.put(o.orderId, o);
        if (o.side == Side.BUY) {
            bids[i].append(o);
            if (i > bestBid) {                  // NONE is -1, so first bid wins
                bestBid = i;
            }
        } else {
            asks[i].append(o);
            if (bestAsk == NONE || i < bestAsk) {
                bestAsk = i;
            }
        }
        bookListener.onAdd(o.orderId, o.side, o.price, o.leaves);
    }

    // --- best price scans ---

    // When the best level empties, walk outward to the next non-empty one.
    // Usually a few cells, because liquidity sits near the best price.

    private int nextAskAtOrAbove(int from) {
        for (int i = from; i < levels; i++) {
            if (!asks[i].isEmpty()) {
                return i;
            }
        }
        return NONE;
    }

    private int nextBidAtOrBelow(int from) {
        for (int i = from; i >= 0; i--) {
            if (!bids[i].isEmpty()) {
                return i;
            }
        }
        return NONE;
    }

    // --- helpers and read-only views ---

    private boolean inBand(long price) {
        return price >= basePrice && price < basePrice + levels;
    }

    private int indexOf(long price) {
        return (int) (price - basePrice);
    }

    public int symbolId() { return symbolId; }

    /** Best bid level, or {@code null} if there are no bids. */
    public PriceLevel bestBid() {
        return bestBid == NONE ? null : bids[bestBid];
    }

    /** Best ask level, or {@code null} if there are no asks. */
    public PriceLevel bestAsk() {
        return bestAsk == NONE ? null : asks[bestAsk];
    }

    /** The level at {@code price} on {@code side}; it may be empty. */
    public PriceLevel level(Side side, long price) {
        if (!inBand(price)) {
            throw new IllegalArgumentException("price out of band: " + price);
        }
        return side == Side.BUY ? bids[indexOf(price)] : asks[indexOf(price)];
    }

    /** A resting order by id, or {@code null}. */
    public Order order(long orderId) {
        return index.get(orderId);
    }

    public int liveOrders() { return index.size(); }

    /** Orders still free in the pool; equals capacity when the book is empty. */
    public int poolAvailable() { return pool.available(); }
}
