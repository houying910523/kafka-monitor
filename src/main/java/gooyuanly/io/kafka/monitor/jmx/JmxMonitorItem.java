package gooyuanly.io.kafka.monitor.jmx;

/**
 * author: hy
 * date: 2019/2/9
 * desc:
 */
public class JmxMonitorItem extends JmxMonitorTemplate {

    private String topic;

    public JmxMonitorItem(String beanName, String attribution, String topic) {
        super(beanName, attribution);
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }
}
