package com.praveen.redisclone.commands;

import com.praveen.redisclone.Command;
import com.praveen.redisclone.KeyValueStore;
import com.praveen.redisclone.RespWriter;

import java.util.List;

/** EXPIRE key seconds -> 1 if TTL was set, 0 if key doesn't exist. */
public class ExpireCommand implements Command {
    @Override
    public String execute(List<String> args, KeyValueStore store) {
        if (args.size() < 2) return RespWriter.error("wrong number of arguments for 'expire' command");
        if (!store.exists(args.get(0))) return RespWriter.integer(0);
        store.expire(args.get(0), Long.parseLong(args.get(1)));
        return RespWriter.integer(1);
    }
}
