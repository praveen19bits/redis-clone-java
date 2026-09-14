package com.praveen.redisclone.commands;

import com.praveen.redisclone.Command;
import com.praveen.redisclone.KeyValueStore;
import com.praveen.redisclone.RespWriter;

import java.util.List;

/** DEL key [key ...] -> integer count of keys actually removed. */
public class DelCommand implements Command {
    @Override
    public String execute(List<String> args, KeyValueStore store) {
        if (args.isEmpty()) return RespWriter.error("wrong number of arguments for 'del' command");
        long removed = args.stream().filter(store::del).count();
        return RespWriter.integer(removed);
    }
}
