package com.ke.bigdata.streaming.kafka.monitor;

import com.ke.bigdata.streaming.kafka.monitor.config.CliOptions;
import com.typesafe.config.Config;

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
        List<Config> configs = new CliOptions(args).getConfigs();

        List<KafkaClusterMonitor> kafkaClusterMonitors = new ArrayList<>();
        for (Config config : configs) {
            KafkaClusterMonitor kafkaClusterMonitor = new KafkaClusterMonitor(config);
            kafkaClusterMonitors.add(kafkaClusterMonitor);
        }

        ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(kafkaClusterMonitors.size());

        for (KafkaClusterMonitor kafkaClusterMonitor : kafkaClusterMonitors) {
            scheduledExecutorService.scheduleAtFixedRate(kafkaClusterMonitor::fetch, 0, 10, TimeUnit.SECONDS);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            scheduledExecutorService.shutdown();
            for (KafkaClusterMonitor kafkaClusterMonitor : kafkaClusterMonitors) {
                kafkaClusterMonitor.close();
            }
        }));
    }
}
