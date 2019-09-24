package gooyuanly.io.kafka.monitor.jmx;

import com.google.common.collect.Lists;
import gooyuanly.io.kafka.monitor.util.IOUtils;
import gooyuanly.io.kafka.monitor.util.Pair;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.net.MalformedURLException;
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

    private JMXConnector connector;
    private final String host;
    private final int port;
    private MBeanServerConnection connection;

    public JmxConnection(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        this.connector = JMXConnectorFactory.connect(new JMXServiceURL(String.format(JMX_URL, host, port)));
        this.connection = connector.getMBeanServerConnection();
    }

    public List<Pair<String, Long>> getAttribution(String beanName, String attr) throws Exception {
        ObjectName on = new ObjectName(beanName);
        if (beanName.contains("*")) {
            Set<ObjectInstance> ois = connection.queryMBeans(on, null);
            return ois.stream().map(oi -> {
                try {
                    Object value = connection.getAttribute(oi.getObjectName(), attr);
                    return new Pair<>(oi.getObjectName().toString(), ((Number) value).longValue());
                } catch (Exception e) {
                    throw new RuntimeException();
                }
            }).collect(Collectors.toList());
        } else {
            long value = ((Number) connection.getAttribute(on, attr)).longValue();
            return Lists.newArrayList(new Pair<>(beanName, value));
        }
    }

    public void close() {
        IOUtils.closeQuietly(connector);
    }

    public static void main(String[] args) throws Exception {
        JmxConnection connection = new JmxConnection("kafka04-matrix.zeus.lianjia.com", 9901);
        List<Pair<String, Long>> objects = connection.getAttribution("kafka.log:type=Log,name=LogEndOffset,topic=search-app-18-log,*", "Value");
        objects.forEach(System.out::println);
    }

    public void reconnect() throws IOException {
        close();
        this.connector = JMXConnectorFactory.connect(new JMXServiceURL(String.format(JMX_URL, host, port)));
        this.connection = connector.getMBeanServerConnection();
    }
}
