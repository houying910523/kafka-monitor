package gooyuanly.io.kafka.monitor.kafka;

/**
 * author: hy
 * date: 2019/2/5
 * desc:
 */
public class TopicPartition {
    private String topic;
    private int partition;

    public TopicPartition(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }
}
