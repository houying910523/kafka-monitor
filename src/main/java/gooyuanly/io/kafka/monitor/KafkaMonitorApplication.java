package gooyuanly.io.kafka.monitor;

import gooyuanly.io.kafka.monitor.config.CliOptions;
import gooyuanly.io.kafka.monitor.reporter.InfluxDbReporter;
import gooyuanly.io.kafka.monitor.reporter.Reporter;
import gooyuanly.io.kafka.monitor.scheduler.Scheduler;
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

        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(parallelism, parallelism, 60, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(), new ThreadFactory() {
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
            scheduler.addTask(() -> {
                long timestamp = System.currentTimeMillis();
                kafkaClusterMonitor.fetch(timestamp);
            });
        }

        scheduler.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                scheduler.stop();
                for (KafkaClusterMonitor kafkaClusterMonitor : kafkaClusterMonitors) {
                    try {
                        kafkaClusterMonitor.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                reporter.close();
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }));
    }
}
