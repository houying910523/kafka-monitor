package com.ke.bigdata.streaming.kafka.monitor.jmx;

import java.util.regex.Pattern;

/**
 * author: hy
 * date: 2019/1/30
 * desc:
 */
public class JmxMonitorTemplate {
    private static final Pattern nameRegex = Pattern.compile(".+[,:]name=.+");
    private final String beanName;
    private final String attribution;

    public JmxMonitorTemplate(String beanName, String attribution) {
        this.beanName = validate(beanName);
        this.attribution = attribution;
    }

    private String validate(String beanName) {
        if (!nameRegex.matcher(beanName).find()) {
            throw new RuntimeException();
        }
        return beanName;
    }

    public String getBeanName() {
        return beanName;
    }

    public String getAttribution() {
        return attribution;
    }

    public JmxMonitorItem applyTopic(String topic) {
        String newBeanName = beanName.replace("${topic}", topic);
        return new JmxMonitorItem(newBeanName, attribution, topic);
    }
}
