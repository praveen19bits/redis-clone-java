package com.praveen.redisclone;

import java.util.List;

/**
 * The Command design pattern: every Redis command (SET, GET, DEL, ...) is
 * its own class implementing this single method. This is exactly how real
 * Redis's source structures commands.c -- a dispatch table mapping command
 * names to handler functions.
 *
 * Why this pattern here specifically:
 *  - Adding a new command later (Phase 3: EXPIRE, INCR, LPUSH...) means
 *    writing one new class and registering it -- zero changes to existing code.
 *  - Each command is independently testable.
 *  - It sets you up naturally for MULTI/EXEC in Phase 3: a transaction is
 *    just a queued List<Command> executed atomically.
 */
public interface Command {
    /**
     * @param args the command arguments EXCLUDING the command name itself,
     *             e.g. for "SET foo bar" this is ["foo", "bar"]
     * @param store the shared data engine
     * @return the fully RESP-encoded reply string, ready to write to the socket
     */
    String execute(List<String> args, KeyValueStore store);
}
