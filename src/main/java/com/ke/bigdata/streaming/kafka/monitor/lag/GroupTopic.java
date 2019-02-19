package com.ke.bigdata.streaming.kafka.monitor.lag;

import java.util.Objects;

/**
 * author: hy
 * date: 2019/2/6
 * desc:
 */
public class GroupTopic {
    private String group;
    private String topic;

    public GroupTopic(String group, String topic) {
        this.group = group;
        this.topic = topic;
    }

    public String group() {
        return group;
    }

    public String topic() {
        return topic;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof GroupTopic))
            return false;
        GroupTopic that = (GroupTopic) o;
        return Objects.equals(group, that.group) && Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, topic);
    }
}
