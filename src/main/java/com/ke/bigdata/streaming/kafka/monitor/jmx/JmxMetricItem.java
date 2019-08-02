package com.ke.bigdata.streaming.kafka.monitor.jmx;

import com.ke.bigdata.streaming.kafka.monitor.util.JsonUtils;
import com.ke.bigdata.streaming.kafka.monitor.util.StringUtils;

/**
 * author: hy
 * date: 2019/1/30
 * desc:
 */
public class JmxMetricItem {
    private String broker;
    private String name;
    private long timestamp;
    private long value;

    public JmxMetricItem(String broker, String name, long timestamp, long value) {
        this.broker = broker;
        this.name = name;
        this.timestamp = timestamp;
        this.value = value;
    }

    public String getBroker() {
        return broker;
    }

    public String getName() {
        return name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getValue() {
        return value;
    }

    public String toCsvString() {
        return StringUtils.join(broker, name, value);
    }
}
