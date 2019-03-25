package com.ke.bigdata.streaming.kafka.monitor;

import com.ke.bigdata.streaming.kafka.monitor.jmx.JmxMonitorTemplate;
import com.ke.bigdata.streaming.kafka.monitor.kafka.KafkaCluster;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
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

    public KafkaClusterMonitor(Config config) throws Exception {
        kafkaCluster = new KafkaCluster(config.getString("zk"), config.getString("path"));

        jmxMonitorTemplates = config.getConfigList("jmx").stream()
                .map(c -> new JmxMonitorTemplate(c.getString("name"), c.getString("beanName"),
                        c.getString("attribution"))).collect(Collectors.toList());

        topics = config.getStringList("topics");
        config.getConfigList("group-topics")
                .forEach(c -> kafkaCluster.registerGroupTopic(c.getString("group"), c.getString("topic")));
    }

    public void fetch() {
        for (String topic : topics) {
            kafkaCluster.fetchJmxItem(jmxMonitorTemplates, topic)
                    .forEach(jmxMetricItem -> logger.info(jmxMetricItem.toString()));
        }
        kafkaCluster.fetchLagItem().forEach(lji -> logger.info(lji.toString()));
    }

    @Override
    public void close() {
        kafkaCluster.close();
    }
}
