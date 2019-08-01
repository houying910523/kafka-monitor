package com.ke.bigdata.streaming.kafka.monitor.jmx;

import com.ke.bigdata.streaming.kafka.monitor.util.IOUtils;
import com.ke.bigdata.streaming.kafka.monitor.util.Pair;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * author: hy
 * date: 2019/1/30
 * desc:
 */
public class JmxConnection {

    private static final String JMX_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";

    private final JMXConnector connector;
    private MBeanServerConnection connection;

    public JmxConnection(String host, int port) throws IOException {
        this.connector = JMXConnectorFactory.connect(new JMXServiceURL(String.format(JMX_URL, host, port)));
        this.connection = connector.getMBeanServerConnection();
    }

    public List<Pair<String, Object>> getAttribution(String beanName, String attr) throws Exception {
        ObjectName on = new ObjectName(beanName);
        Set<ObjectInstance> ois = connection.queryMBeans(on, null);
        return ois.stream().map(oi -> {
            try {
                return new Pair<>(oi.getObjectName().toString(), connection.getAttribute(oi.getObjectName(), attr));
            } catch (Exception e) {
                throw new RuntimeException();
            }
        }).collect(Collectors.toList());
    }

    public void close() {
        IOUtils.closeQuietly(connector);
    }

    public static void main(String[] args) throws Exception {
        JmxConnection connection = new JmxConnection("kafka04-matrix.zeus.lianjia.com", 9901);
        List<Pair<String, Object>> objects = connection.getAttribution("kafka.log:type=Log,name=LogEndOffset,topic=search-app-18-log,*", "Value");
        objects.forEach(System.out::println);
    }
}
