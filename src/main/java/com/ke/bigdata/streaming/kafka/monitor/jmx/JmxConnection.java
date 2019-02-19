package com.ke.bigdata.streaming.kafka.monitor.jmx;

import com.ke.bigdata.streaming.kafka.monitor.util.IOUtils;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;

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

    public Object getAttribution(String beanName, String attr) throws Exception {
        ObjectName on = new ObjectName(beanName);
        return connection.getAttribute(on, attr);
    }

    public void close() {
        IOUtils.closeQuietly(connector);
    }
}
