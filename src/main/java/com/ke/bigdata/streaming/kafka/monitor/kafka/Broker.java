package com.ke.bigdata.streaming.kafka.monitor.kafka;

import com.ke.bigdata.streaming.kafka.monitor.jmx.JmxConnection;
import com.ke.bigdata.streaming.kafka.monitor.jmx.JmxMetricItem;
import com.ke.bigdata.streaming.kafka.monitor.jmx.JmxMonitorItem;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: hy
 * date: 2019/1/30
 * desc:
 */
public class Broker {
    private int id;
    private String host;
    private int port;
    private int jmxPort;
    private JmxConnection jmxConnection;

    public Broker(int id, String host, int port, int jmxPort) throws Exception {
        this.id = id;
        this.host = host;
        this.port = port;
        this.jmxPort = jmxPort;
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

    public int getJmxPort() {
        return jmxPort;
    }

    public List<JmxMetricItem> poll(List<JmxMonitorItem> list) {
        return list.stream().map(item -> {
            try {
                Object value = jmxConnection.getAttribution(item.getBeanName(), item.getAttribution());
                return new JmxMetricItem(id, host, item.getName(), item.getTopic(), value);

            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }).collect(Collectors.toList());
    }

    public void close() {
        jmxConnection.close();
    }
}
