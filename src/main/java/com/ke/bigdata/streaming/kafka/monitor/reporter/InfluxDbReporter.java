package com.ke.bigdata.streaming.kafka.monitor.reporter;

import com.ke.bigdata.streaming.kafka.monitor.jmx.JmxMetricItem;
import com.ke.bigdata.streaming.kafka.monitor.lag.GroupTopicPartition;
import com.ke.bigdata.streaming.kafka.monitor.lag.LagMetricItem;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author hy
 * @date 2019/8/2
 * @desc
 */
public class InfluxDbReporter implements Reporter {
    private static final Logger logger = LoggerFactory.getLogger(InfluxDbReporter.class);
    private final InfluxDB influxDB;

    public InfluxDbReporter(String address) {
        this.influxDB = InfluxDBFactory.connect("http://" + address);
        influxDB.setDatabase("kafka_monitor");
        influxDB.enableBatch();
    }

    @Override
    public void report(JmxMetricItem jmxMetricItem) {
        String[] array = jmxMetricItem.getName().split(":", 2)[1].split(",");
        Point.Builder builder = Point.measurement("kafka_monitor")
                .tag("broker", jmxMetricItem.getBroker())
                .time(jmxMetricItem.getTimestamp(), TimeUnit.MILLISECONDS);

        boolean setField = false;
        for (String kv: array) {
            if (kv.contains("=")) {
                String[] keyAndValue = kv.split("=", 2);
                if (keyAndValue[0].equals("name")) {
                    builder.addField(keyAndValue[1], jmxMetricItem.getValue());
                    setField = true;
                } else {
                    builder.tag(keyAndValue[0], keyAndValue[1]);
                }
            }
        }

        if (!setField) {
            logger.warn("ObjectName: {} has not name property", jmxMetricItem.getName());
            return;
        }
        Point point = builder.build();
        logger.info(point.toString());
        influxDB.write(point);
    }

    @Override
    public void report(LagMetricItem lagMetricItem) {
        Point point = Point.measurement("kafka_monitor")
                .tag("topic", lagMetricItem.getTopic())
                .tag("partition", String.valueOf(lagMetricItem.getPartition()))
                .tag("group", lagMetricItem.getGroup())
                .time(lagMetricItem.getCommitTime(), TimeUnit.MILLISECONDS)
                .addField("lag", lagMetricItem.getLag())
                .build();
        logger.info(point.toString());
        influxDB.write(point);
    }

    @Override
    public void flush() throws IOException {
        influxDB.flush();
    }

    @Override
    public void close() throws IOException {
        influxDB.close();
    }
}
