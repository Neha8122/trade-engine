package com.tradeengine.raft;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.CRC32C;

/**
 * Raft's durable state in three files in one directory.
 *
 * <p><b>{@code meta}</b>: term and vote. Two fixed 32-byte slots written
 * alternately, each with a version number and a checksum. A crash halfway
 * through a write can only damage the slot being written; on startup the
 * valid slot with the higher version wins, so the previous term and vote
 * survive.
 *
 * <p><b>{@code log}</b>: append-only records, one per entry:
 * {@code [length u32][crc32c u32][index u64][term u64][command bytes]}. On
 * startup the file is read front to back; the first record that is
 * incomplete, fails its checksum or breaks the index sequence marks where a
 * crash cut a write short, and the file is truncated there. Those entries
 * were never acknowledged (we fsync before replying), so dropping them is
 * safe.
 *
 * <p><b>{@code snapshot}</b>: {@code [crc u32][index u64][term u64][length u32][data]},
 * the state machine as of {@code index}. Written to a temp file, fsynced
 * and renamed into place, so it is always either the old snapshot or the
 * new one, never half of one. The log is then rewritten without the entries
 * the snapshot covers. Because log records carry their index, a crash
 * between those two steps is harmless: on load, records at or below the
 * snapshot index are skipped.
 *
 * <p>Every change is fsynced before the method returns, except log appends
 * and truncations in group-commit mode, which wait for {@link #sync()}.
 *
 * <p>Note: on macOS, {@code FileChannel.force} is {@code fsync}, which
 * doesn't flush the drive's own cache ({@code F_FULLFSYNC} would). On Linux
 * it reaches the disk.
 */
public final class FileStorage implements Storage, AutoCloseable {

    private static final int SLOT = 32;
    private static final int SLOT_BODY = 20;        // version 8 + term 8 + vote 4
    private static final int RECORD_HEADER = 8;     // length 4 + crc 4
    private static final int RECORD_FIXED = 16;     // index 8 + term 8

    private final Path dir;
    private final FileChannel meta;
    private volatile FileChannel log;           // read by the sync thread
    private final boolean groupCommit;
    private boolean dirty;

    // Async group commit (docs/lld-snapshots.html §3). Each counter has one
    // writer: `mutations` and `requested` the event loop, `durable` the sync
    // thread. No locks.
    private final boolean async;
    private long mutations;                     // log changes made so far
    private volatile long requested;            // ask the sync thread to reach this
    private volatile long durable;              // the sync thread has fsynced up to here
    private volatile boolean closing;
    private volatile Runnable onDurable;
    private Thread syncThread;

    private long metaVersion;
    private long currentTerm;
    private int votedFor = -1;

    private long snapshotIndex;
    private long snapshotTerm;
    private byte[] snapshotData;

    private final List<LogEntry> entries = new ArrayList<>();   // entries.get(0) is snapshotIndex + 1
    private long[] offsets = new long[1024];                     // file offset of each entry
    private long logEnd;

    public FileStorage(Path dir) {
        this(dir, false);
    }

    /**
     * Group commit: log appends and truncations are written but not fsynced
     * until {@link #sync()}, so one fsync covers everything a server did in
     * one event-loop turn. The server calls {@code sync()} before sending
     * any message, so nobody is ever told about an entry that isn't on disk.
     * Term, vote and snapshot changes are always fsynced at once.
     */
    public static FileStorage groupCommit(Path dir) {
        return new FileStorage(dir, true, false);
    }

    /**
     * Group commit with the fsync on a background thread: {@link #requestSync()}
     * returns at once and the event loop keeps working while the disk
     * flushes; {@link #isDurable} says when a turn's writes are safe to
     * acknowledge.
     */
    public static FileStorage asyncGroupCommit(Path dir) {
        return new FileStorage(dir, true, true);
    }

    private FileStorage(Path dir, boolean groupCommit) {
        this(dir, groupCommit, false);
    }

    private FileStorage(Path dir, boolean groupCommit, boolean async) {
        this.dir = dir;
        this.groupCommit = groupCommit;
        this.async = async;
        try {
            Files.createDirectories(dir);
            meta = FileChannel.open(dir.resolve("meta"),
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            loadMeta();
            loadSnapshot();
            log = openLog();
            recoverLog();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // --- term and vote ---

    @Override public long currentTerm() { return currentTerm; }
    @Override public int votedFor() { return votedFor; }

    @Override
    public void saveTermAndVote(long term, int votedFor) {
        long version = metaVersion + 1;
        ByteBuffer b = ByteBuffer.allocate(SLOT);
        b.putLong(version).putLong(term).putInt(votedFor);
        b.putInt(crc(b.array(), 0, SLOT_BODY));
        b.clear();
        try {
            writeFully(meta, b, (version & 1) * SLOT);   // alternate slots
            meta.force(false);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        metaVersion = version;
        currentTerm = term;
        this.votedFor = votedFor;
    }

    private void loadMeta() throws IOException {
        ByteBuffer b = ByteBuffer.allocate(2 * SLOT);
        meta.read(b, 0);
        byte[] a = b.array();
        for (int slot = 0; slot < 2; slot++) {
            int at = slot * SLOT;
            ByteBuffer s = ByteBuffer.wrap(a, at, SLOT);
            long version = s.getLong();
            long term = s.getLong();
            int vote = s.getInt();
            int crc = s.getInt();
            if (version > metaVersion && crc == crc(a, at, SLOT_BODY)) {
                metaVersion = version;
                currentTerm = term;
                votedFor = vote;
            }
        }
    }

    // --- log ---

    @Override public long lastIndex() { return snapshotIndex + entries.size(); }

    @Override
    public long termAt(long index) {
        if (index == snapshotIndex) {
            return snapshotTerm;                // 0 for index 0 with no snapshot
        }
        return entry(index).term();
    }

    @Override
    public LogEntry entry(long index) {
        if (index <= snapshotIndex) {
            throw new IllegalArgumentException("index " + index + " is compacted into the snapshot");
        }
        return entries.get((int) (index - snapshotIndex - 1));
    }

    @Override
    public void append(List<LogEntry> batch) {
        if (batch.isEmpty()) {
            return;
        }
        long first = lastIndex() + 1;
        ByteBuffer b = encode(batch, first);
        long[] starts = new long[batch.size()];
        long pos = logEnd;
        for (int i = 0; i < batch.size(); i++) {
            starts[i] = pos;
            pos += RECORD_HEADER + RECORD_FIXED + batch.get(i).command().length;
        }
        try {
            writeFully(log, b, logEnd);
            forceLog();                         // one fsync for the whole batch (or turn)
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        for (int i = 0; i < batch.size(); i++) {
            addOffset(starts[i]);
            entries.add(batch.get(i));
        }
        logEnd = pos;
    }

    @Override
    public void truncateFrom(long index) {
        int from = (int) (index - snapshotIndex - 1);
        long cut = offsets[from];
        try {
            log.truncate(cut);
            forceLog();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        entries.subList(from, entries.size()).clear();
        logEnd = cut;
    }

    /** Reads every intact record; cuts the file at the first damaged one. */
    private void recoverLog() throws IOException {
        long size = log.size();
        long pos = 0;
        boolean skippedPrefix = false;
        ByteBuffer header = ByteBuffer.allocate(RECORD_HEADER);
        while (pos + RECORD_HEADER <= size) {
            header.clear();
            readFully(log, header, pos);
            int bodyLen = header.getInt(0);
            int crc = header.getInt(4);
            if (bodyLen < RECORD_FIXED || pos + RECORD_HEADER + bodyLen > size) {
                break;                          // torn write at the tail
            }
            ByteBuffer body = ByteBuffer.allocate(bodyLen);
            readFully(log, body, pos + RECORD_HEADER);
            if (crc != crc(body.array(), 0, bodyLen)) {
                break;                          // damaged record
            }
            long index = body.getLong(0);
            long next = pos + RECORD_HEADER + bodyLen;
            if (index <= snapshotIndex) {
                // Already covered by the snapshot: a crash hit between writing
                // the snapshot and rewriting the log. Skip it.
                skippedPrefix = true;
                pos = next;
                continue;
            }
            if (index != lastIndex() + 1) {
                break;                          // gap: can't trust anything after it
            }
            byte[] command = Arrays.copyOfRange(body.array(), RECORD_FIXED, bodyLen);
            addOffset(pos);
            entries.add(new LogEntry(body.getLong(8), command));
            pos = next;
        }
        if (pos < size) {
            log.truncate(pos);
            log.force(false);
        }
        logEnd = pos;
        if (skippedPrefix) {
            rewriteLog(new ArrayList<>(entries));   // finish the interrupted compaction
        }
    }

    private ByteBuffer encode(List<LogEntry> batch, long firstIndex) {
        int bytes = 0;
        for (LogEntry e : batch) {
            bytes += RECORD_HEADER + RECORD_FIXED + e.command().length;
        }
        ByteBuffer b = ByteBuffer.allocate(bytes);
        for (int i = 0; i < batch.size(); i++) {
            LogEntry e = batch.get(i);
            int bodyLen = RECORD_FIXED + e.command().length;
            int bodyStart = b.position() + RECORD_HEADER;
            b.putInt(bodyLen).putInt(0).putLong(firstIndex + i).putLong(e.term()).put(e.command());
            b.putInt(bodyStart - 4, crc(b.array(), bodyStart, bodyLen));
        }
        b.flip();
        return b;
    }

    // --- snapshot ---

    @Override public long snapshotIndex() { return snapshotIndex; }
    @Override public long snapshotTerm() { return snapshotTerm; }
    @Override public byte[] snapshotData() { return snapshotData; }

    @Override
    public void installSnapshot(long index, long term, byte[] data) {
        if (index <= snapshotIndex) {
            return;                             // we already have a newer one
        }
        boolean keepSuffix = index <= lastIndex() && termAt(index) == term;
        List<LogEntry> suffix = keepSuffix
                ? new ArrayList<>(entries.subList((int) (index - snapshotIndex), entries.size()))
                : List.of();
        try {
            // 1. The snapshot file, atomically: temp file, fsync, rename.
            ByteBuffer b = ByteBuffer.allocate(24 + data.length);
            b.putInt(0).putLong(index).putLong(term).putInt(data.length).put(data);
            b.putInt(0, crc(b.array(), 4, 20 + data.length));
            b.flip();
            Path tmp = dir.resolve("snapshot.tmp");
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
                writeFully(ch, b, 0);
                ch.force(true);
            }
            Files.move(tmp, dir.resolve("snapshot"), StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            syncDirectory();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        snapshotIndex = index;
        snapshotTerm = term;
        snapshotData = data;
        // 2. The log, without what the snapshot covers.
        rewriteLog(suffix);
    }

    private void loadSnapshot() throws IOException {
        Path file = dir.resolve("snapshot");
        if (!Files.exists(file)) {
            return;
        }
        byte[] a = Files.readAllBytes(file);
        ByteBuffer b = ByteBuffer.wrap(a);
        int crc = b.getInt();
        long index = b.getLong();
        long term = b.getLong();
        int len = b.getInt();
        if (a.length != 24 + len || crc != crc(a, 4, 20 + len)) {
            // Only ever replaced by an atomic rename, so this is real damage.
            throw new IOException("snapshot file is corrupt: " + file);
        }
        snapshotIndex = index;
        snapshotTerm = term;
        snapshotData = Arrays.copyOfRange(a, 24, 24 + len);
    }

    /** Replaces the log file with exactly {@code keep}, atomically. */
    private void rewriteLog(List<LogEntry> keep) {
        try {
            Path tmp = dir.resolve("log.tmp");
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
                if (!keep.isEmpty()) {
                    writeFully(ch, encode(keep, snapshotIndex + 1), 0);
                }
                ch.force(true);
            }
            log.close();
            Files.move(tmp, dir.resolve("log"), StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            syncDirectory();
            log = openLog();
            dirty = false;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        entries.clear();
        long pos = 0;
        for (LogEntry e : keep) {
            addOffset(pos);
            entries.add(e);
            pos += RECORD_HEADER + RECORD_FIXED + e.command().length;
        }
        logEnd = pos;
    }

    private FileChannel openLog() throws IOException {
        return FileChannel.open(dir.resolve("log"),
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    /** Makes a rename durable. Not every platform lets Java fsync a directory. */
    private void syncDirectory() {
        try (FileChannel d = FileChannel.open(dir, StandardOpenOption.READ)) {
            d.force(true);
        } catch (IOException ignored) {
            // best effort: e.g. unsupported for directories on this platform
        }
    }

    // --- group commit ---

    private void forceLog() throws IOException {
        if (groupCommit) {
            dirty = true;
            mutations++;
        } else {
            log.force(false);
        }
    }

    @Override
    public long requestSync() {
        if (!async) {
            sync();
            return 0;
        }
        if (mutations > requested) {
            requested = mutations;
            if (syncThread == null) {
                syncThread = new Thread(this::syncLoop, "fsync-" + dir.getFileName());
                syncThread.setDaemon(true);
                syncThread.start();
            }
            LockSupport.unpark(syncThread);
        }
        return mutations;
    }

    @Override
    public boolean isDurable(long token) {
        return durable >= token;
    }

    @Override
    public void onDurable(Runnable callback) {
        this.onDurable = callback;
    }

    /** The sync thread: fsync whenever the loop has asked for more than is durable. */
    private void syncLoop() {
        while (!closing) {
            long target = requested;
            if (target <= durable) {
                LockSupport.parkNanos(1_000_000);   // woken by unpark; the timeout is a safety net
                continue;
            }
            try {
                log.force(false);
            } catch (IOException e) {
                // The loop swapped the log file (snapshot compaction) while we
                // were flushing the old one. Try again on the new one before
                // claiming anything is durable.
                continue;
            }
            durable = target;
            Runnable callback = onDurable;
            if (callback != null) {
                callback.run();
            }
        }
    }

    @Override
    public void sync() {
        if (dirty) {
            try {
                log.force(false);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            dirty = false;
        }
    }

    // --- helpers ---

    private void addOffset(long offset) {
        int i = entries.size();
        if (i == offsets.length) {
            offsets = Arrays.copyOf(offsets, i * 2);
        }
        offsets[i] = offset;
    }

    private static int crc(byte[] a, int off, int len) {
        CRC32C c = new CRC32C();
        c.update(a, off, len);
        return (int) c.getValue();
    }

    private static void writeFully(FileChannel ch, ByteBuffer b, long at) throws IOException {
        while (b.hasRemaining()) {
            at += ch.write(b, at);
        }
    }

    private static void readFully(FileChannel ch, ByteBuffer b, long at) throws IOException {
        while (b.hasRemaining()) {
            int n = ch.read(b, at + b.position());
            if (n < 0) {
                throw new IOException("unexpected end of file");
            }
        }
    }

    @Override
    public void close() throws IOException {
        closing = true;
        if (syncThread != null) {
            LockSupport.unpark(syncThread);
            try {
                syncThread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        meta.close();
        log.close();
    }
}
