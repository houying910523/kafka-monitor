package com.ke.bigdata.streaming.kafka.monitor;

import com.ke.bigdata.streaming.kafka.monitor.config.CliOptions;
import com.ke.bigdata.streaming.kafka.monitor.reporter.InfluxDbReporter;
import com.ke.bigdata.streaming.kafka.monitor.reporter.Reporter;
import com.ke.bigdata.streaming.kafka.monitor.scheduler.Scheduler;
import com.typesafe.config.Config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * author: hy
 * date: 2019/1/30
 * desc:
 */

public class KafkaMonitorApplication {

    public static void main(String[] args) throws Exception {
        CliOptions cliOptions = new CliOptions(args);

        List<Config> configs = cliOptions.getConfigs();
        String reportHost = cliOptions.getReporter();
        int parallelism = cliOptions.getParallelism();

        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(parallelism, parallelism, 0, TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(), new ThreadFactory() {
            int i = 0;
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("worker-" + i);
                i++;
                return thread;
            }
        });
        Reporter reporter = new InfluxDbReporter(reportHost);
        List<KafkaClusterMonitor> kafkaClusterMonitors = new ArrayList<>();
        for (Config config : configs) {
            KafkaClusterMonitor kafkaClusterMonitor = new KafkaClusterMonitor(config, threadPoolExecutor);
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
