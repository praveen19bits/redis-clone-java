package com.praveen.redisclone.commands;

import com.praveen.redisclone.Command;
import com.praveen.redisclone.KeyValueStore;
import com.praveen.redisclone.RespWriter;

import java.util.List;

/** EXISTS key -> 1 or 0 */
public class ExistsCommand implements Command {
    @Override
    public String execute(List<String> args, KeyValueStore store) {
        if (args.isEmpty()) return RespWriter.error("wrong number of arguments for 'exists' command");
        return RespWriter.integer(store.exists(args.get(0)) ? 1 : 0);
    }
}
