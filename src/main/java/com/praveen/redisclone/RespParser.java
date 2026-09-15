package com.praveen.redisclone;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses the RESP (REdis Serialization Protocol) wire format.
 *
 * Real Redis clients (redis-cli, Jedis, etc.) send commands as RESP Arrays
 * of Bulk Strings. Example: SET foo bar is sent over the wire as:
 *
 *   *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
 *
 *   *3        -> array of 3 elements
 *   $3\r\nSET -> bulk string of length 3: "SET"
 *   $3\r\nfoo -> bulk string of length 3: "foo"
 *   $3\r\nbar -> bulk string of length 3: "bar"
 *
 * This class is stateful PER CONNECTION: because we're using non-blocking
 * NIO, a full command can arrive split across multiple TCP reads. So we
 * accumulate bytes in an internal buffer and only return a parsed command
 * once we've seen a complete one. This mirrors exactly the problem real
 * event-loop servers (including Redis itself) have to solve.
 */
public class RespParser {

    // Mirrors real Redis's proto-max-multibulk-len / proto-max-bulk-len
    // defaults. Without these caps, a client-declared length is trusted
    // as-is: `new ArrayList<>(arrayLen)` or `new byte[bulkLen]` with a
    // multi-billion value throws an uncaught OutOfMemoryError (an Error,
    // not an Exception -- it escapes both this class's and the reactor
    // loop's `catch (Exception ...)` and kills the whole process in
    // --mode=epoll, since that's the only thread keeping the JVM alive).
    private static final int MAX_ARRAY_LEN = 1_048_576;
    private static final int MAX_BULK_LEN = 512 * 1024 * 1024;
    // Hard ceiling on how much this one connection can make us buffer.
    // Without it, a client can declare a huge-but-legal bulk length and
    // trickle bytes in slowly, forcing unbounded per-connection growth
    // (ensureCapacity has no other limit) until the server runs out of
    // memory across a handful of such connections.
    private static final int MAX_BUFFER_SIZE = MAX_BULK_LEN + 4096;

    /**
     * Thrown when the client has sent bytes that can never become a valid
     * RESP command (wrong marker byte, non-numeric length, or a length
     * outside the sane bounds above) -- as opposed to returning null, which
     * means "valid so far, just need more bytes". Extends IOException so it
     * flows through the exact same "this connection is dead, close it" path
     * both servers already use for real I/O failures, with no change needed
     * to their method signatures.
     */
    public static class ProtocolException extends IOException {
        public ProtocolException(String message) {
            super(message);
        }
    }

    // Accumulates bytes across multiple socket reads until we have a full command.
    private ByteBuffer accumulator = ByteBuffer.allocate(4096);

    /** Feed newly-read bytes from the socket into the parser's buffer. */
    public void feed(ByteBuffer newData) throws ProtocolException {
        ensureCapacity(newData.remaining());
        accumulator.put(newData);
    }

    private void ensureCapacity(int extra) throws ProtocolException {
        if (accumulator.remaining() < extra) {
            // long math: (position + extra) * 2 could itself overflow a 32-bit
            // int for adversarial "extra" values before we ever get to compare
            // it against MAX_BUFFER_SIZE.
            long newSize = (long) (accumulator.position() + extra) * 2;
            if (newSize > MAX_BUFFER_SIZE) {
                throw new ProtocolException("command too large to buffer (" + newSize + " bytes)");
            }
            ByteBuffer bigger = ByteBuffer.allocate((int) newSize);
            accumulator.flip();
            bigger.put(accumulator);
            accumulator = bigger;
        }
    }

    /**
     * Try to parse ONE complete command from whatever bytes we've buffered so far.
     * Returns null if we don't yet have a full command (caller should wait for
     * more data from the socket before calling again). Throws ProtocolException
     * if the buffered bytes can never form a valid command -- the caller should
     * close the connection rather than call this again.
     */
    public List<String> tryParseCommand() throws ProtocolException {
        accumulator.flip(); // switch to read mode
        int startPos = accumulator.position();

        if (!accumulator.hasRemaining()) {
            accumulator.compact();
            return null; // nothing buffered yet
        }

        byte first = accumulator.get();
        if (first != '*') {
            // Not a RESP array -> and we don't support inline commands (Phase
            // 1). This byte can never become valid no matter how much more
            // data arrives, so we fail fast instead of the old
            // rewind-and-wait, which replayed this exact same byte forever
            // and left the connection stuck open with no way to progress.
            throw new ProtocolException("expected '*', got '" + (char) first + "'");
        }

        Integer arrayLen = readInteger();
        if (arrayLen == null) { rewind(startPos); return null; } // incomplete
        if (arrayLen > MAX_ARRAY_LEN) {
            throw new ProtocolException("invalid multibulk length " + arrayLen);
        }
        if (arrayLen <= 0) {
            // *0 (empty array) or *-1 (null array): nothing more to read for
            // this command. Must still compact() here -- returning early
            // without it left the buffer in "read mode" with unconsumed
            // leftover bytes, which the next feed()'s put() would silently
            // corrupt.
            accumulator.compact();
            return new ArrayList<>();
        }

        List<String> parts = new ArrayList<>(arrayLen);
        for (int i = 0; i < arrayLen; i++) {
            if (!accumulator.hasRemaining()) { rewind(startPos); return null; }
            byte marker = accumulator.get();
            if (marker != '$') {
                throw new ProtocolException("expected '$', got '" + (char) marker + "'");
            }

            Integer bulkLen = readInteger();
            if (bulkLen == null) { rewind(startPos); return null; }
            if (bulkLen < 0 || bulkLen > MAX_BULK_LEN) {
                throw new ProtocolException("invalid bulk length " + bulkLen);
            }

            // bulkLen is now bounded by MAX_BULK_LEN, so "+ 2" can never
            // overflow -- previously bulkLen was unbounded, and a
            // near-Integer.MAX_VALUE value wrapped this sum negative,
            // silently bypassing the "wait for more data" check and falling
            // straight into `new byte[bulkLen]` on a single malformed message.
            if (accumulator.remaining() < bulkLen + 2) { rewind(startPos); return null; } // wait for more bytes

            byte[] strBytes = new byte[bulkLen];
            accumulator.get(strBytes);
            accumulator.get(); accumulator.get(); // consume trailing \r\n
            parts.add(new String(strBytes, StandardCharsets.UTF_8));
        }

        // Successfully parsed a full command -> compact buffer, keeping leftovers.
        accumulator.compact();
        return parts;
    }

    /**
     * Reads a CRLF-terminated integer, e.g. the "3" in "*3\r\n". Returns null
     * if incomplete (caller should wait for more bytes). Throws
     * ProtocolException if the field is present but not a valid integer --
     * that can never resolve itself by waiting for more data.
     */
    private Integer readInteger() throws ProtocolException {
        StringBuilder sb = new StringBuilder();
        while (true) {
            if (!accumulator.hasRemaining()) return null;
            byte b = accumulator.get();
            if (b == '\r') {
                if (!accumulator.hasRemaining()) return null;
                accumulator.get(); // consume \n
                try {
                    return Integer.parseInt(sb.toString());
                } catch (NumberFormatException notANumber) {
                    throw new ProtocolException("invalid length field '" + sb + "'");
                }
            }
            sb.append((char) b);
        }
    }

    private void rewind(int startPos) {
        accumulator.position(startPos);
        accumulator.compact();
    }
}
