package com.praveen.redisclone;

import com.praveen.redisclone.commands.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps command names -> Command implementations, and routes incoming
 * commands to the right one. This is the "invoker" side of the Command
 * pattern -- it doesn't know HOW each command works, only WHERE to send it.
 *
 * This class is also where you'd add cross-cutting concerns later
 * (logging, metrics, auth checks) as a Decorator wrapping each Command,
 * without touching the commands themselves.
 */
public class CommandDispatcher {

    private final Map<String, Command> commands = new HashMap<>();
    private final KeyValueStore store;

    public CommandDispatcher(KeyValueStore store) {
        this.store = store;
        register("PING", new PingCommand());
        register("ECHO", new EchoCommand());
        register("SET", new SetCommand());
        register("GET", new GetCommand());
        register("DEL", new DelCommand());
        register("EXISTS", new ExistsCommand());
        register("EXPIRE", new ExpireCommand());
        register("SLOWCOMPUTE", new SlowComputeCommand());
    }

    private void register(String name, Command command) {
        commands.put(name.toUpperCase(), command);
    }

    /** parts[0] is the command name, parts[1..] are its arguments. */
    public String dispatch(List<String> parts) {
        if (parts.isEmpty()) return RespWriter.error("empty command");

        String commandName = parts.get(0).toUpperCase();
        Command command = commands.get(commandName);

        if (command == null) {
            return RespWriter.error("unknown command '" + parts.get(0) + "'");
        }

        List<String> args = parts.subList(1, parts.size());
        return command.execute(args, store);
    }
}
