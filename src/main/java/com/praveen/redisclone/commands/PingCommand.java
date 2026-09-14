package com.praveen.redisclone.commands;

import com.praveen.redisclone.Command;
import com.praveen.redisclone.KeyValueStore;
import com.praveen.redisclone.RespWriter;

import java.util.List;

/** PING -> PONG, or PING <message> -> <message>. Used by clients as a health check. */
public class PingCommand implements Command {
    @Override
    public String execute(List<String> args, KeyValueStore store) {
        if (args.isEmpty()) return RespWriter.simpleString("PONG");
        return RespWriter.bulkString(args.get(0));
    }
}
