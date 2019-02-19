package com.ke.bigdata.streaming.kafka.monitor.jmx;

/**
 * author: hy
 * date: 2019/2/9
 * desc:
 */
public class JmxMonitorItem extends JmxMonitorTemplate {

    private String topic;

    public JmxMonitorItem(String name, String beanName, String attribution, String topic) {
        super(name, beanName, attribution);
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }
}
