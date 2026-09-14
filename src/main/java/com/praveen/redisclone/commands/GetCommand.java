package com.praveen.redisclone.commands;

import com.praveen.redisclone.Command;
import com.praveen.redisclone.KeyValueStore;
import com.praveen.redisclone.RespWriter;

import java.util.List;

/** GET key -> value, or a null bulk string if the key doesn't exist / has expired. */
public class GetCommand implements Command {
    @Override
    public String execute(List<String> args, KeyValueStore store) {
        if (args.isEmpty()) return RespWriter.error("wrong number of arguments for 'get' command");
        String value = store.get(args.get(0));
        return RespWriter.bulkString(value); // bulkString(null) correctly emits $-1\r\n
    }
}
