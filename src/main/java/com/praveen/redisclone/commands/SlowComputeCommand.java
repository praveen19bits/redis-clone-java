package com.praveen.redisclone.commands;

import com.praveen.redisclone.Command;
import com.praveen.redisclone.KeyValueStore;
import com.praveen.redisclone.RespWriter;

import java.util.List;

/**
 * SLOWCOMPUTE <millis> -> DONE, after burning real CPU for approximately
 * that long. This stands in for the "matrix computation" example: genuine
 * CPU-bound work, not just a sleep, so it faithfully occupies whichever
 * thread runs it -- exactly like a real scientific-computing workload would.
 */
public class SlowComputeCommand implements Command {
    @Override
    public String execute(List<String> args, KeyValueStore store) {
        long millis = args.isEmpty() ? 1000 : Long.parseLong(args.get(0));
        long deadline = System.currentTimeMillis() + millis;
        long x = 0;
        // Tight busy loop -- real CPU burn, not Thread.sleep, so it can't be
        // "optimized away" as idle waiting. This IS the thread being busy.
        while (System.currentTimeMillis() < deadline) {
            x += (x * 31 + 7) % 104729; // pointless arithmetic, just to burn cycles
        }
        return RespWriter.simpleString("DONE:" + x % 10); // %10 just to use x, avoid dead-code elimination
    }
}
