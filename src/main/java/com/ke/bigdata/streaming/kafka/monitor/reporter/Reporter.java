package com.ke.bigdata.streaming.kafka.monitor.reporter;

import com.ke.bigdata.streaming.kafka.monitor.jmx.JmxMetricItem;
import com.ke.bigdata.streaming.kafka.monitor.lag.LagMetricItem;

import java.io.Closeable;
import java.io.Flushable;

/**
 * @author hy
 * @date 2019/8/2
 * @desc
 */
public interface Reporter extends Closeable, Flushable {

    void report(JmxMetricItem jmxMetricItem);

    void report(LagMetricItem lji);
}
