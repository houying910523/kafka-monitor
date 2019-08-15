package gooyuanly.io.kafka.monitor.lag;

import gooyuanly.io.kafka.monitor.kafka.TopicPartition;

import java.util.Objects;

/**
 * author: hy
 * date: 2019/2/5
 * desc:
 */
public class GroupTopicPartition extends TopicPartition {
    private String group;

    public GroupTopicPartition(String group, String topic, int partition) {
        super(topic, partition);
        this.group = group;
    }

    public String group() {
        return group;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GroupTopicPartition)) {
            return false;
        }
        GroupTopicPartition that = (GroupTopicPartition) o;
        return partition() == that.partition() && Objects.equals(group, that.group) && Objects
                .equals(topic(), that.topic());
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, topic(), partition());
    }
}
