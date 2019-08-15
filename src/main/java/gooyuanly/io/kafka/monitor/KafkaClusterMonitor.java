package gooyuanly.io.kafka.monitor;

import gooyuanly.io.kafka.monitor.jmx.JmxMetricItem;
import gooyuanly.io.kafka.monitor.jmx.JmxMonitorTemplate;
import gooyuanly.io.kafka.monitor.kafka.KafkaCluster;
import gooyuanly.io.kafka.monitor.lag.LagMetricItem;
import gooyuanly.io.kafka.monitor.reporter.Reporter;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

/**
 * @author hy
 * @date 2019/2/9
 * @desc
 */
public class KafkaClusterMonitor implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaClusterMonitor.class);
    private final KafkaCluster kafkaCluster;
    private final List<JmxMonitorTemplate> jmxMonitorTemplates;
    private final List<String> topics;
    private final String clusterName;
    private final ThreadPoolExecutor poolExecutor;
    private Reporter reporter;

    public KafkaClusterMonitor(Config config, ThreadPoolExecutor poolExecutor) throws Exception {
        this.clusterName = config.getString("name");
        this.kafkaCluster = new KafkaCluster(config.getString("zk"), config.getString("path"));

        this.jmxMonitorTemplates = config.getConfigList("jmx").stream()
                .map(c -> new JmxMonitorTemplate(c.getString("beanName"),
                        c.getString("attribution"))).collect(Collectors.toList());

        this.topics = config.getStringList("topics");
        config.getConfigList("group-topics")
                .forEach(c -> kafkaCluster.registerGroupTopic(c.getString("group"), c.getString("topic")));
        this.poolExecutor = poolExecutor;
    }

    public void fetch(long timestamp) {
        logger.info("fetch cluster {} metrics start", clusterName);
        for (String topic : topics) {
            poolExecutor.execute(() -> {
                logger.info("fetch cluster {} jmx metrics", clusterName);
                List<JmxMetricItem> jmxMetricItems = kafkaCluster.fetchJmxItem(jmxMonitorTemplates, topic, timestamp);
                jmxMetricItems.forEach(jmxMetricItem -> {
                    reporter.report(clusterName, jmxMetricItem);
                });
            });
        }
        poolExecutor.execute(() -> {
            logger.info("fetch cluster {} lags", clusterName);
            List<LagMetricItem> lagMetricItems = kafkaCluster.fetchLagItem();
            lagMetricItems.forEach(lji -> {
                reporter.report(clusterName, lji);
            });
        });
        logger.info("queue.size = {}", poolExecutor.getQueue().size());
    }

    @Override
    public void close() throws IOException {
        kafkaCluster.close();
        reporter.close();
    }

    public void setReporter(Reporter reporter) {
        this.reporter = reporter;
    }
}
