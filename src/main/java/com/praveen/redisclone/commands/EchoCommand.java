package com.praveen.redisclone.commands;

import com.praveen.redisclone.Command;
import com.praveen.redisclone.KeyValueStore;
import com.praveen.redisclone.RespWriter;

import java.util.List;

/** ECHO <message> -> <message>. Trivial command, good smoke test for the parser. */
public class EchoCommand implements Command {
    @Override
    public String execute(List<String> args, KeyValueStore store) {
        if (args.isEmpty()) return RespWriter.error("wrong number of arguments for 'echo' command");
        return RespWriter.bulkString(args.get(0));
    }
}
