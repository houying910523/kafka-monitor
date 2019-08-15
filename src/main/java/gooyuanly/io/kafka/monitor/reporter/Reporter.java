package gooyuanly.io.kafka.monitor.reporter;

import gooyuanly.io.kafka.monitor.jmx.JmxMetricItem;
import gooyuanly.io.kafka.monitor.lag.LagMetricItem;

import java.io.Closeable;
import java.io.Flushable;

/**
 * @author hy
 * @date 2019/8/2
 * @desc
 */
public interface Reporter extends Closeable, Flushable {

    void report(String clusterName, JmxMetricItem jmxMetricItem);

    void report(String clusterName, LagMetricItem lagMetricItem);
}
