package com.praveen.redisclone;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * THE REACTOR PATTERN -- this is the architectural core of the whole project.
 *
 * Real Redis is famously single-threaded: one thread runs an event loop that
 * multiplexes thousands of client connections using OS-level I/O
 * notification (epoll on Linux, kqueue on macOS). It never blocks waiting
 * on any one client -- it asks the OS "which of these sockets are ready to
 * read/write RIGHT NOW?" and only touches those.
 *
 * Java's java.nio.channels.Selector is the portable equivalent of
 * epoll/kqueue. This class is deliberately built the same way Redis is:
 * ONE thread, ONE Selector, non-blocking sockets. No thread-per-connection,
 * no thread pool. That's the whole point of the exercise -- you FEEL why
 * this scales to many connections without the cost of thousands of OS threads,
 * and you feel WHY a single slow command (e.g. a huge KEYS scan) can stall
 * every other client, exactly like it does in real Redis.
 *
 * Flow per iteration of the loop:
 *   1. selector.select() blocks until >=1 channel is ready for I/O
 *   2. for each ready channel: accept new connections, or read/write data
 *   3. repeat forever
 */
public class EventLoopServer {

    private final int port;
    private final KeyValueStore store = new KeyValueStore();
    private final CommandDispatcher dispatcher = new CommandDispatcher(store);

    // Per-connection parser state -- each client socket gets its own RespParser
    // because commands can arrive split across multiple reads (see RespParser).
    private final Map<SocketChannel, RespParser> parsers = new HashMap<>();

    public EventLoopServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        Selector selector = Selector.open();

        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        // Explicit backlog: without this, bind() falls back to a small
        // implementation default. A burst of many simultaneous connection
        // attempts (e.g. a load test that opens N clients with no ramping)
        // can exceed that default before the accept loop drains it, causing
        // the OS to actively refuse the excess -- exactly the error being
        // debugged here. 1024 gives real headroom for burst connects.
        serverChannel.bind(new InetSocketAddress(port), 1024);
        serverChannel.configureBlocking(false); // CRITICAL: non-blocking mode is what makes the reactor possible
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        System.out.println("redis-clone-java [epoll/event-loop mode] listening on port " + port
                + " -- 1 OS thread total, regardless of connection count");

        while (true) {
            selector.select(); // blocks here until something is ready -- this IS the event loop

            var readyKeys = selector.selectedKeys().iterator();
            while (readyKeys.hasNext()) {
                SelectionKey key = readyKeys.next();
                readyKeys.remove(); // must remove manually, Selector doesn't do it for you

                try {
                    if (!key.isValid()) continue;
                    if (key.isAcceptable()) handleAccept(key, selector);
                    else if (key.isReadable()) handleRead(key);
                } catch (Exception unexpected) {
                    // CRITICAL: this is the ONLY thread running the whole server.
                    // An uncaught exception here -- of ANY kind, not just IOException --
                    // kills this thread, and with no other non-daemon thread alive,
                    // silently kills the entire JVM process. A single client's
                    // transient failure (e.g. a connection reset between the OS
                    // marking a socket "acceptable" and this code calling accept()
                    // on it -- a real race under a burst of simultaneous connects)
                    // must never be allowed to take down every other connection.
                    //
                    // Only attempt cleanup if this key actually belongs to a client
                    // socket -- the accept key's channel is a ServerSocketChannel,
                    // not a SocketChannel, and blindly casting it (the original bug
                    // here) throws a ClassCastException that this same catch block
                    // then failed to catch, crashing the reactor permanently.
                    if (key.channel() instanceof SocketChannel) {
                        closeConnection(key);
                    } else {
                        System.err.println("Error on non-client key (e.g. accept failure): " + unexpected);
                    }
                }
            }
        }
    }

    private void handleAccept(SelectionKey key, Selector selector) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel client = serverChannel.accept();
        client.configureBlocking(false);
        client.register(selector, SelectionKey.OP_READ);
        parsers.put(client, new RespParser());
        System.out.println("client connected: " + client.getRemoteAddress());
    }

    private void handleRead(SelectionKey key) throws IOException {
        SocketChannel client = (SocketChannel) key.channel();
        ByteBuffer buffer = ByteBuffer.allocate(4096);

        int bytesRead = client.read(buffer);
        if (bytesRead == -1) { // client closed the connection
            closeConnection(key);
            return;
        }

        buffer.flip();
        RespParser parser = parsers.get(client);
        parser.feed(buffer);

        // A single TCP read can contain multiple pipelined commands (this is
        // how real Redis pipelining works) -- so drain ALL complete commands
        // currently buffered, not just one.
        List<String> commandParts;
        while ((commandParts = parser.tryParseCommand()) != null) {
            if (commandParts.isEmpty()) continue;
            String reply = dispatcher.dispatch(commandParts);
            client.write(ByteBuffer.wrap(reply.getBytes(StandardCharsets.UTF_8)));
        }
    }

    private void closeConnection(SelectionKey key) {
        try {
            SocketChannel client = (SocketChannel) key.channel();
            System.out.println("client disconnected: " + client.getRemoteAddress());
            parsers.remove(client);
            key.cancel();
            client.close();
        } catch (IOException ignored) {
            // already gone
        }
    }
}
