package com.tradeengine.ringbuffer;

/** Called by the consumer thread for each message it drains. */
@FunctionalInterface
public interface EventHandler<E> {

    /**
     * @param event    the slot object; only valid during this call, because
     *                 the producer reuses it once the batch is freed
     * @param sequence the message's position in the stream, 0, 1, 2, ...
     */
    void onEvent(E event, long sequence);
}
