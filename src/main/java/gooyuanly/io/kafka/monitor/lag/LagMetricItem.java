package gooyuanly.io.kafka.monitor.lag;

import gooyuanly.io.kafka.monitor.util.JsonUtils;
import gooyuanly.io.kafka.monitor.util.StringUtils;

/**
 * author: hy
 * date: 2019/2/5
 * desc:
 */
public class LagMetricItem {
    private String group;
    private String topic;
    private int partition;
    private long lag;
    private long commitTime;

    public LagMetricItem(GroupTopicPartition groupTopicPartition, long lag, long commitTime) {
        this.group = groupTopicPartition.group();
        this.topic = groupTopicPartition.topic();
        this.partition = groupTopicPartition.partition();
        this.lag = lag;
        this.commitTime = commitTime;
    }

    public long getLag() {
        return lag;
    }

    public long getCommitTime() {
        return commitTime;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public String getGroup() {
        return group;
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }

    public String toCsvString() {
        return StringUtils.join(group, topic, partition, commitTime, lag);
    }
}
