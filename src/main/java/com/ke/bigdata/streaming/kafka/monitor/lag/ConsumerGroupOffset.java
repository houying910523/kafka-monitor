package com.ke.bigdata.streaming.kafka.monitor.lag;

/**
 * author: hy
 * date: 2019/2/6
 * desc:
 */
public class ConsumerGroupOffset {
    private GroupTopicPartition groupTopicPartition;
    private long offset;
    private long commitTime;

    public ConsumerGroupOffset(GroupTopicPartition groupTopicPartition, long offset, long commitTime) {
        this.groupTopicPartition = groupTopicPartition;
        this.offset = offset;
        this.commitTime = commitTime;
    }

    public long offset() {
        return offset;
    }

    public long commitTime() {
        return commitTime;
    }

    public String topic() {
        return groupTopicPartition.topic();
    }

    public int partition() {
        return groupTopicPartition.partition();
    }

    public String group() {
        return groupTopicPartition.group();
    }

    public GroupTopicPartition groupTopicPartition() {
        return groupTopicPartition;
    }

    @Override
    public String toString() {
        return "LagMetricItem{" + "group=" + group() + ", topic=" + topic() + ", partition=" + partition() + ", offset="
                + offset + ", commitTime=" + commitTime + '}';
    }
}
