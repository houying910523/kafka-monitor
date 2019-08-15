package gooyuanly.io.kafka.monitor.kafka;

import gooyuanly.io.kafka.monitor.jmx.JmxConnection;
import gooyuanly.io.kafka.monitor.jmx.JmxMetricItem;
import gooyuanly.io.kafka.monitor.jmx.JmxMonitorItem;
import gooyuanly.io.kafka.monitor.util.Pair;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author: hy
 * date: 2019/1/30
 * desc:
 */
public class Broker {
    private int id;
    private String host;
    private int port;
    private JmxConnection jmxConnection;

    public Broker(int id, String host, int port, int jmxPort) throws Exception {
        this.id = id;
        this.host = host;
        this.port = port;
        if (jmxPort == -1) {
            throw new Exception("Jmx port is -1, kafka broker: " + host);
        }
        this.jmxConnection = new JmxConnection(host, jmxPort);
    }

    public int getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public List<JmxMetricItem> poll(List<JmxMonitorItem> list, long timestamp) {
        return list.stream().flatMap(item -> {
            try {
                List<Pair<String, Long>> values = jmxConnection.getAttribution(item.getBeanName(), item.getAttribution());
                return values.stream().map(value -> new JmxMetricItem(host, value.getLeft(), timestamp, value.getRight()));
            } catch (Exception e) {
                e.printStackTrace();
                return Stream.empty();
            }
        }).collect(Collectors.toList());
    }

    public void close() {
        jmxConnection.close();
    }
}
