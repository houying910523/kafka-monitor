package com.ke.bigdata.streaming.kafka.monitor.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

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
    private final String reporter;

    public CliOptions(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("c", "config", true, "配置目录");
        options.addOption(null, "reporter", true, "influxdb地址");
        CommandLine commandLine = new DefaultParser().parse(options, args);
        configDir = commandLine.getOptionValue("config");
        reporter = commandLine.getOptionValue("reporter");
    }

    public List<Config> getConfigs() throws Exception {
        File file = new File(configDir);
        if (!file.exists()) {
            throw new FileNotFoundException(configDir);
        }
        if (!file.isDirectory()) {
            throw new NotDirectoryException(file.getPath() + " is not a directory");
        }
        File[] jsonFiles = file.listFiles(pathname -> pathname.isFile() && pathname.getName().endsWith(".json"));
        assert jsonFiles != null;
        return Arrays.stream(jsonFiles).map(ConfigFactory::parseFile).collect(Collectors.toList());
    }

    public String getReporter() {
        return reporter;
    }
}
