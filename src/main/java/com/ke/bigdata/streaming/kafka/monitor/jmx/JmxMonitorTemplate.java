package com.ke.bigdata.streaming.kafka.monitor.jmx;

/**
 * author: hy
 * date: 2019/1/30
 * desc:
 */
public class JmxMonitorTemplate {
    private final String name;
    private final String beanName;
    private final String attribution;

    public JmxMonitorTemplate(String name, String beanName, String attribution) {
        this.name = name;
        this.beanName = beanName;
        this.attribution = attribution;
    }

    public String getName() {
        return name;
    }

    public String getBeanName() {
        return beanName;
    }

    public String getAttribution() {
        return attribution;
    }

    public JmxMonitorItem applyTopic(String topic) {
        String newBeanName = beanName.replace("${topic}", topic);
        return new JmxMonitorItem(name, newBeanName, attribution, topic);
    }
}
