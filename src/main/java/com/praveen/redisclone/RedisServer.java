package com.praveen.redisclone;

import java.io.IOException;

/**
 * Entry point. Picks which concurrency model to run via a command-line flag,
 * so you can A/B the two architectures against the identical protocol
 * parser, command dispatcher, and data store -- the ONLY thing that changes
 * between runs is how connections are scheduled.
 *
 * Usage:
 *   java -cp out com.praveen.redisclone.RedisServer --mode=thread [--port=6380]
 *   java -cp out com.praveen.redisclone.RedisServer --mode=epoll  [--port=6380]
 *
 * Defaults to epoll mode if --mode is omitted.
 */
public class RedisServer {

    public static void main(String[] args) throws IOException {
        String mode = "epoll";
        int port = 6380;

        for (String arg : args) {
            if (arg.startsWith("--mode=")) mode = arg.substring("--mode=".length());
            else if (arg.startsWith("--port=")) port = Integer.parseInt(arg.substring("--port=".length()));
        }

        // Prints exactly what the JVM sees -- this is how you CONFIRM a CPU/memory
        // constraint (e.g. Docker's --cpus=1) actually took effect, rather than
        // assuming it did. Modern JVMs (JDK 10+) read cgroup limits automatically,
        // so availableProcessors() reflects the container's real cap, not the host's.
        long maxHeapMb = Runtime.getRuntime().maxMemory() / (1024 * 1024);
        System.out.println("JVM sees " + Runtime.getRuntime().availableProcessors()
                + " CPU core(s) available, max heap " + maxHeapMb + " MB");

        switch (mode) {
            case "thread" -> new ThreadPerConnectionServer(port).start();
            case "epoll" -> new EventLoopServer(port).start();
            default -> {
                System.err.println("Unknown mode '" + mode + "'. Use --mode=thread or --mode=epoll");
                System.exit(1);
            }
        }
    }
}
