package com.praveen.redisclone;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * THE TRADITIONAL / NAIVE MODEL -- one blocking OS thread per connection.
 *
 * This is deliberately the "before" picture. Every client that connects
 * gets its own java.lang.Thread, which does a BLOCKING read() and just
 * sits there -- consuming an OS thread stack (~1MB default) and taking up
 * a slot in the OS scheduler -- for as long as the connection is open,
 * even if that client never sends another byte. 1,000 idle clients = 1,000
 * idle-but-fully-provisioned threads.
 *
 * Notice this class reuses RespParser UNCHANGED from EventLoopServer. The
 * parser doesn't know or care whether it's fed by a non-blocking Selector
 * loop or a blocking per-thread read loop -- parsing the protocol and
 * scheduling I/O are genuinely separate concerns. That separation is what
 * lets us swap the whole concurrency model without touching the protocol
 * or command-dispatch code at all.
 */
public class ThreadPerConnectionServer {

    private final int port;
    private final KeyValueStore store = new KeyValueStore();
    private final CommandDispatcher dispatcher = new CommandDispatcher(store);

    public ThreadPerConnectionServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        // Same explicit-backlog fix as EventLoopServer -- see comment there.
        ServerSocket serverSocket = new ServerSocket(port, 1024);
        System.out.println("redis-clone-java [thread-per-connection mode] listening on port " + port
                + " -- spawns 1 OS thread per connected client");

        while (true) {
            Socket client = serverSocket.accept(); // blocks here until a NEW client connects
            // The defining move of this model: hand this client its OWN thread
            // and immediately go back to accepting the next one. That thread
            // now exists for the ENTIRE lifetime of this connection.
            Thread clientThread = new Thread(() -> handleClient(client));
            clientThread.start();
        }
    }

    private void handleClient(Socket client) {
        RespParser parser = new RespParser();
        try (InputStream in = client.getInputStream();
             OutputStream out = client.getOutputStream()) {

            byte[] readBuffer = new byte[4096];
            while (true) {
                int bytesRead = in.read(readBuffer); // BLOCKS this thread until data arrives
                if (bytesRead == -1) break; // client closed the connection

                parser.feed(ByteBuffer.wrap(readBuffer, 0, bytesRead));

                List<String> commandParts;
                while ((commandParts = parser.tryParseCommand()) != null) {
                    if (commandParts.isEmpty()) continue;
                    String reply = dispatcher.dispatch(commandParts);
                    out.write(reply.getBytes(StandardCharsets.UTF_8));
                }
            }
        } catch (IOException clientDisconnectedAbruptly) {
            // normal on connection reset -- nothing to do
        } finally {
            try { client.close(); } catch (IOException ignored) { }
        }
    }
}
