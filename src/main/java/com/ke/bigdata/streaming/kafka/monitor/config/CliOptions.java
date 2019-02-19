package com.ke.bigdata.streaming.kafka.monitor.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.NotDirectoryException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * author: hy
 * date: 2019/2/9
 * desc:
 */
public class CliOptions {

    private final String configDir;

    public CliOptions(String[] args) throws FileNotFoundException, NotDirectoryException {
        configDir = args[0];
        File file = new File(configDir);
        if (!file.exists()) {
            throw new FileNotFoundException(configDir);
        }
        if (!file.isDirectory()) {
            throw new NotDirectoryException(file.getPath() + " is not a directory");
        }
    }

    public List<Config> getConfigs() {
        File file = new File(configDir);
        File[] jsonFiles = file.listFiles(pathname -> pathname.isFile() && pathname.getName().endsWith(".json"));
        assert jsonFiles != null;
        return Arrays.stream(jsonFiles)
                .map(ConfigFactory::parseFile)
                .collect(Collectors.toList());
    }
}
