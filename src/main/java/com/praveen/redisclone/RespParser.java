package com.praveen.redisclone;

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

    // Accumulates bytes across multiple socket reads until we have a full command.
    private ByteBuffer accumulator = ByteBuffer.allocate(4096);

    /** Feed newly-read bytes from the socket into the parser's buffer. */
    public void feed(ByteBuffer newData) {
        ensureCapacity(newData.remaining());
        accumulator.put(newData);
    }

    private void ensureCapacity(int extra) {
        if (accumulator.remaining() < extra) {
            ByteBuffer bigger = ByteBuffer.allocate((accumulator.position() + extra) * 2);
            accumulator.flip();
            bigger.put(accumulator);
            accumulator = bigger;
        }
    }

    /**
     * Try to parse ONE complete command from whatever bytes we've buffered so far.
     * Returns null if we don't yet have a full command (caller should wait for
     * more data from the socket before calling again).
     */
    public List<String> tryParseCommand() {
        accumulator.flip(); // switch to read mode
        int startPos = accumulator.position();

        try {
            if (!accumulator.hasRemaining()) {
                return null; // nothing buffered yet
            }

            byte first = accumulator.get();
            if (first != '*') {
                // Not a RESP array -> treat rest of buffer as garbage/inline command.
                // Real Redis supports "inline commands" too; we skip that for Phase 1.
                accumulator.position(startPos);
                accumulator.compact();
                return null;
            }

            Integer arrayLen = readInteger();
            if (arrayLen == null) { rewind(startPos); return null; } // incomplete
            if (arrayLen <= 0) { return new ArrayList<>(); }

            List<String> parts = new ArrayList<>(arrayLen);
            for (int i = 0; i < arrayLen; i++) {
                if (!accumulator.hasRemaining()) { rewind(startPos); return null; }
                byte marker = accumulator.get();
                if (marker != '$') { rewind(startPos); return null; } // malformed

                Integer bulkLen = readInteger();
                if (bulkLen == null) { rewind(startPos); return null; }

                if (accumulator.remaining() < bulkLen + 2) { rewind(startPos); return null; } // wait for more bytes

                byte[] strBytes = new byte[bulkLen];
                accumulator.get(strBytes);
                accumulator.get(); accumulator.get(); // consume trailing \r\n
                parts.add(new String(strBytes, StandardCharsets.UTF_8));
            }

            // Successfully parsed a full command -> compact buffer, keeping leftovers.
            accumulator.compact();
            return parts;

        } catch (Exception malformedOrIncomplete) {
            rewind(startPos);
            return null;
        }
    }

    /** Reads a CRLF-terminated integer, e.g. the "3" in "*3\r\n". Returns null if incomplete. */
    private Integer readInteger() {
        StringBuilder sb = new StringBuilder();
        while (true) {
            if (!accumulator.hasRemaining()) return null;
            byte b = accumulator.get();
            if (b == '\r') {
                if (!accumulator.hasRemaining()) return null;
                accumulator.get(); // consume \n
                return Integer.parseInt(sb.toString());
            }
            sb.append((char) b);
        }
    }

    private void rewind(int startPos) {
        accumulator.position(startPos);
        accumulator.compact();
    }
}
