package com.ke.bigdata.streaming.kafka.monitor.kafka;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.ke.bigdata.streaming.kafka.monitor.jmx.JmxMetricItem;
import com.ke.bigdata.streaming.kafka.monitor.jmx.JmxMonitorItem;
import com.ke.bigdata.streaming.kafka.monitor.jmx.JmxMonitorTemplate;
import com.ke.bigdata.streaming.kafka.monitor.lag.LagMetricItem;
import com.ke.bigdata.streaming.kafka.monitor.lag.LagService;
import com.ke.bigdata.streaming.kafka.monitor.util.IOUtils;
import kafka.admin.AdminClient;
import org.apache.kafka.clients.*;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.clients.consumer.internals.RequestFuture;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.utils.Time;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
/**
 * author: hy
 * date: 2019/1/30
 * desc:
 */
public class KafkaCluster implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaCluster.class);
    private ZkUtils zkUtils;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Map<String, Broker> brokerMap;
    private String bootstrap;

    private LagService lagService;

    private ConsumerNetworkClient client;
    public KafkaCluster(String zk, String path) throws Exception {
        this.zkUtils = new ZkUtils(zk, path);
        initBrokerInfo();
        bootstrap = brokerMap.values().stream().map(b -> b.getHost() + ":" + b.getPort()).reduce((b1, b2) -> b1 + "," + b2).get();
        lagService = new LagService(bootstrap);
        lagService.start();
        client = createConsumerNetworkClient();
    }

    public List<JmxMetricItem> fetchJmxItem(List<JmxMonitorTemplate> list, String topic) {
        List<LeaderPartition> leaderPartitions = zkUtils.getPartitions(topic);
        Set<String> leaders = leaderPartitions.stream()
                .map(LeaderPartition::leader)
                .collect(Collectors.toSet());
        List<JmxMonitorItem> items = list.stream()
                .map(item -> item.applyTopic(topic))
                .collect(Collectors.toList());
        return leaders.stream()
                .map(id -> brokerMap.get(id))
                .flatMap(broker -> broker.poll(items).stream())
                .collect(Collectors.toList());
    }

    public void registerGroupTopic(String group, String topic) {
        lagService.registerGroupTopic(group, topic);
    }

    public List<LagMetricItem> fetchLagItem() {
        return lagService.snapshot();
    }

    public List<String> listGroups(String topic) {
        return brokerMap.values().stream().map(broker -> new Node(Integer.valueOf(broker.getId()), broker.getHost(), broker.getPort()))
                .flatMap(node -> {
                    RequestFuture<ClientResponse> future = client.send(node, new ListGroupsRequest.Builder());
                    client.poll(future);
                    ListGroupsResponse response = (ListGroupsResponse) future.value().responseBody();
                    List<String> groups = response.groups().stream().map(ListGroupsResponse.Group::groupId).collect(Collectors.toList());
                    future = client.send(node, new DescribeGroupsRequest.Builder(groups));
                    client.poll(future);
                    Set<String> result = Sets.newHashSet();
                    DescribeGroupsResponse response2 = (DescribeGroupsResponse) future.value().responseBody();
                    response2.groups().forEach( (group, metadata) -> {
                        if ("Stable".equals(metadata.state())) {
                            Set<String> topics = metadata.members().stream()
                                    .flatMap(gm -> {
                                        PartitionAssignor.Assignment assignment = ConsumerProtocol.deserializeAssignment(gm.memberAssignment());
                                        return assignment.partitions().stream().map(TopicPartition::topic);
                                    }).collect(Collectors.toSet());
                            if (topics.contains(topic)) {
                                result.add(group);
                            }
                        }
                    });
                    return result.stream();
                }).collect(Collectors.toList());
    }

    @Override
    public void close() {
        brokerMap.values().forEach(Broker::close);
        lagService.stop();
        IOUtils.closeQuietly(client);
    }

    private void initBrokerInfo() throws Exception {
        logger.info("init brokers");
        brokerMap = Maps.newHashMap();
        for(String id: zkUtils.listBrokerIds()) {
            Broker broker = brokerInfo(id);
            brokerMap.put(id, broker);
            logger.info("init broker[{}]", broker.getHost());
        }
    }

    private Broker brokerInfo(String id) throws Exception {
        String json = zkUtils.readBrokerInfo(id);
        JsonNode jsonNode = objectMapper.readTree(json);
        String host = jsonNode.get("host").asText();
        int port = jsonNode.get("port").asInt();
        return new Broker(id, host, port, jsonNode.get("jmx_port").asInt());
    }

    private ConsumerNetworkClient createConsumerNetworkClient() {
        Map<String, Object> map = Maps.newHashMap();
        map.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        map.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL);

        Time time = Time.SYSTEM;
        Metrics metrics = new Metrics(time);
        Metadata metadata = new Metadata();
        ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(map);

        List<InetSocketAddress> brokerAddresses = ClientUtils.parseAndValidateAddresses(brokerMap.values().stream().map(b -> b.getHost() + ":" + b.getPort()).collect(Collectors.toList()));
        Cluster bootstrapCluster = Cluster.bootstrap(brokerAddresses);
        metadata.update(bootstrapCluster, 0);

        int defaultConnectionMaxIdleMs = 9 * 60 * 1000;
        int defaultRequestTimeoutMs = 5000;
        int defaultMaxInFlightRequestsPerConnection = 100;
        int defaultReconnectBackoffMs = 50;
        int defaultSendBufferBytes = 128 * 1024;
        int defaultRetryBackoffMs = 100;

        int defaultReceiveBufferBytes = 32 * 1024;
        AtomicInteger adminClientIdSequence = new AtomicInteger(1);

        Selector selector = new Selector(
                defaultConnectionMaxIdleMs,
                metrics,
                time,
                "admin",
                channelBuilder);

        NetworkClient networkClient = new NetworkClient(
                selector,
                metadata,
                "admin-" + adminClientIdSequence.getAndIncrement(),
                defaultMaxInFlightRequestsPerConnection,
                defaultReconnectBackoffMs,
                defaultSendBufferBytes,
                defaultReceiveBufferBytes,
                defaultRequestTimeoutMs,
                time,
                true);

        ConsumerNetworkClient highLevelClient = new ConsumerNetworkClient(
                networkClient,
                metadata,
                time,
                defaultRetryBackoffMs,
                defaultRequestTimeoutMs);

        return highLevelClient;
    }
}
