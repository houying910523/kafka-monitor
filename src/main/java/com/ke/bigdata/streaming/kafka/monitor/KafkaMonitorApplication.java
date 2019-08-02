package com.ke.bigdata.streaming.kafka.monitor;

import com.ke.bigdata.streaming.kafka.monitor.config.CliOptions;
import com.ke.bigdata.streaming.kafka.monitor.reporter.InfluxDbReporter;
import com.ke.bigdata.streaming.kafka.monitor.reporter.Reporter;
import com.ke.bigdata.streaming.kafka.monitor.scheduler.Scheduler;
import com.typesafe.config.Config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * author: hy
 * date: 2019/1/30
 * desc:
 */

public class KafkaMonitorApplication {

    public static void main(String[] args) throws Exception {
        CliOptions cliOptions = new CliOptions(args);
        List<Config> configs = cliOptions.getConfigs();

        List<KafkaClusterMonitor> kafkaClusterMonitors = new ArrayList<>();

        Reporter reporter = new InfluxDbReporter(cliOptions.getReporter());

        for (Config config : configs) {
            KafkaClusterMonitor kafkaClusterMonitor = new KafkaClusterMonitor(config);
            kafkaClusterMonitor.setReporter(reporter);
            kafkaClusterMonitors.add(kafkaClusterMonitor);
        }

        Scheduler scheduler = new Scheduler(10, TimeUnit.SECONDS);

        for (KafkaClusterMonitor kafkaClusterMonitor : kafkaClusterMonitors) {
            scheduler.addTask(kafkaClusterMonitor::fetch);
        }

        scheduler.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                scheduler.stop();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for (KafkaClusterMonitor kafkaClusterMonitor : kafkaClusterMonitors) {
                try {
                    kafkaClusterMonitor.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }));
    }
}
