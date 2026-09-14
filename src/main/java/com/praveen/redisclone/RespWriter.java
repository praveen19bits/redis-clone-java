package com.praveen.redisclone;

import java.nio.charset.StandardCharsets;

/**
 * Encodes responses into RESP wire format. This is the inverse of RespParser.
 *
 * RESP has 5 core reply types, each identified by its first byte:
 *   +OK\r\n                -> Simple String
 *   -Error message\r\n     -> Error
 *   :1000\r\n              -> Integer
 *   $6\r\nfoobar\r\n       -> Bulk String
 *   *2\r\n...\r\n...\r\n   -> Array
 *   $-1\r\n                -> Null bulk string (the "key not found" reply)
 */
public class RespWriter {

    public static String simpleString(String s) {
        return "+" + s + "\r\n";
    }

    public static String error(String message) {
        return "-ERR " + message + "\r\n";
    }

    public static String integer(long value) {
        return ":" + value + "\r\n";
    }

    public static String bulkString(String s) {
        if (s == null) return nullBulkString();
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        return "$" + bytes.length + "\r\n" + s + "\r\n";
    }

    public static String nullBulkString() {
        return "$-1\r\n";
    }

    public static String array(String... elements) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(elements.length).append("\r\n");
        for (String e : elements) sb.append(e);
        return sb.toString();
    }
}
