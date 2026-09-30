package com.tradeengine.server;

import java.io.IOException;
import java.net.ProtocolException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

/**
 * Cuts a TCP byte stream back into frames: {@code [u32 length][u8 type][fields]}.
 *
 * <p>TCP has no message boundaries. One read can return half a frame, or
 * three and a half. This keeps the unfinished tail in its buffer and hands
 * out only complete frames, however the bytes were split.
 */
public final class FrameReader {

    /** Receives one complete frame; {@code buf} is only valid during the call. */
    @FunctionalInterface
    public interface FrameHandler {
        /** @param at index of the type byte; {@code length} counts type + fields */
        void onFrame(ByteBuffer buf, int at, int length) throws IOException;
    }

    private final int maxFrame;
    private ByteBuffer buf;

    public FrameReader(int maxFrame) {
        this.maxFrame = maxFrame;
        this.buf = ByteBuffer.allocate(Math.min(64 * 1024, maxFrame + 4)).order(ByteOrder.LITTLE_ENDIAN);
    }

    /** Reads whatever the channel has. Returns bytes read, or -1 at end of stream. */
    public int readFrom(ReadableByteChannel channel) throws IOException {
        if (!buf.hasRemaining()) {
            grow();                             // a big frame is only partly here
        }
        return channel.read(buf);
    }

    /** Hands every complete frame to {@code handler}; returns how many. */
    public int drain(FrameHandler handler) throws IOException {
        buf.flip();
        int frames = 0;
        try {
            while (buf.remaining() >= 4) {
                int length = buf.getInt(buf.position());
                if (length < 1 || length > maxFrame) {
                    throw new ProtocolException("bad frame length " + length);
                }
                if (buf.remaining() < 4 + length) {
                    break;                      // rest of this frame not here yet
                }
                int at = buf.position() + 4;
                handler.onFrame(buf, at, length);
                buf.position(at + length);
                frames++;
            }
        } finally {
            buf.compact();                      // keep the partial frame, if any
        }
        return frames;
    }

    private void grow() throws ProtocolException {
        if (buf.capacity() >= maxFrame + 4) {
            throw new ProtocolException("frame larger than " + maxFrame);
        }
        ByteBuffer bigger = ByteBuffer.allocate(Math.min(buf.capacity() * 2, maxFrame + 4))
                .order(ByteOrder.LITTLE_ENDIAN);
        buf.flip();
        bigger.put(buf);
        buf = bigger;
    }
}
