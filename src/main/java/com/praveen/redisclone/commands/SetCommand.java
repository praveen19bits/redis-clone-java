package com.praveen.redisclone.commands;

import com.praveen.redisclone.Command;
import com.praveen.redisclone.KeyValueStore;
import com.praveen.redisclone.RespWriter;

import java.util.List;

/** SET key value -> OK */
public class SetCommand implements Command {
    @Override
    public String execute(List<String> args, KeyValueStore store) {
        if (args.size() < 2) return RespWriter.error("wrong number of arguments for 'set' command");
        store.set(args.get(0), args.get(1));
        return RespWriter.simpleString("OK");
    }
}
