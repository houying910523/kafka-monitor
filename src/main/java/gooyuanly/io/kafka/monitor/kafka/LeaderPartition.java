package gooyuanly.io.kafka.monitor.kafka;

/**
 * author: hy
 * date: 2019/1/30
 * desc:
 */
public class LeaderPartition extends TopicPartition {
    private String leader;

    public LeaderPartition(String topic, int partition, String leader) {
        super(topic, partition);
        this.leader = leader;
    }

    public String leader() {
        return leader;
    }
}
