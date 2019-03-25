package com.ke.bigdata.streaming.kafka.monitor.jmx;

import com.ke.bigdata.streaming.kafka.monitor.util.JsonUtils;

/**
 * author: hy
 * date: 2019/1/30
 * desc:
 */
public class JmxMetricItem {
    private int id;
    private String broker;
    private String name;
    private String topic;
    private Object value;

    public JmxMetricItem(int id, String broker, String name, String topic, Object value) {
        this.id = id;
        this.broker = broker;
        this.name = name;
        this.topic = topic;
        this.value = value;
    }

    public String getBroker() {
        return broker;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getTopic() {
        return topic;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
