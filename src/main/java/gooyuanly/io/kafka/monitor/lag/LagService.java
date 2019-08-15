package gooyuanly.io.kafka.monitor.lag;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import kafka.common.MessageFormatter;
import kafka.coordinator.GroupMetadataManager;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * author: hy
 * date: 2019/2/5
 * desc:
 */
public class LagService {
    private static final Logger logger = LoggerFactory.getLogger(LagService.class);
    private final Pattern pattern;
    private final ExecutorService pool;
    private volatile boolean threadStop = true;
    private Map<String, Set<String>> topicGroups;
    private Set<Integer> currentPartition;

    private KafkaConsumer<byte[], byte[]> kafkaConsumer;

    private Map<GroupTopicPartition, ConsumerGroupOffset> snapshot;

    public LagService(String bootstrap) {
        this.topicGroups = Maps.newConcurrentMap();
        this.currentPartition = Sets.newConcurrentHashSet();
        this.pattern = Pattern.compile(
                "^\\[([0-9a-zA-Z\\-_]+),([0-9a-zA-Z\\-_]+),(\\d+)]::\\[OffsetMetadata\\[(\\d+),.+CommitTime (\\d+).+$");
        this.pool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1), r -> {
            Thread thread = new Thread(r);
            thread.setName("offset-consumer-thread");
            return thread;
        });

        Map<String, Object> params = Maps.newHashMap();
        params.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        params.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-offset-monitor-group");
        params.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        params.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        params.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        params.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        this.kafkaConsumer = new KafkaConsumer<>(params);
        this.snapshot = Maps.newHashMap();
    }

    public void start() {
        pool.execute(createRunnable());
    }

    public void stop() throws InterruptedException {
        threadStop = true;
        pool.awaitTermination(3, TimeUnit.SECONDS);
        pool.shutdownNow();
    }

    public synchronized void registerGroupTopic(String group, String topic) {
        Set<String> groups = topicGroups.get(topic);
        if (groups == null) {
            groups = Sets.newConcurrentHashSet();
            topicGroups.put(topic, groups);
        }
        groups.add(group);
        logger.info("add ({}, {})", group, topic);
        int pid = Math.abs(group.hashCode() % 50);
        currentPartition.add(pid);
    }

    private Runnable createRunnable() {
        return () -> {
            threadStop = false;
            logger.info("loop start");
            kafkaConsumer.subscribe(Collections.singletonList("__consumer_offsets"));
            MessageFormatter formatter = new GroupMetadataManager.OffsetsMessageFormatter();
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(1024);
            while (!threadStop) {
                ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(500);
                currentPartition.forEach(partition -> {
                    TopicPartition tp = new TopicPartition("__consumer_offsets", partition);
                    for (ConsumerRecord<byte[], byte[]> record : records.records(tp)) {
                        formatter.writeTo(record, new PrintStream(byteArrayOutputStream));
                        String line = new String(byteArrayOutputStream.toByteArray());
                        parseLine(line);
                        byteArrayOutputStream.reset();
                    }
                });
            }
            logger.info("loop exit");
        };
    }

    public List<LagMetricItem> snapshot() {
        List<ConsumerGroupOffset> list = Lists.newArrayList(snapshot.values());
        List<TopicPartition> topicPartitions = list.stream()
                .map(gtp -> new TopicPartition(gtp.topic(), gtp.partition())).collect(Collectors.toList());
        Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(topicPartitions);
        return list.stream().map(cgo -> {
            long endOffset = endOffsets.get(new TopicPartition(cgo.topic(), cgo.partition()));
            long consumerOffset = cgo.offset();
            long lag = endOffset - consumerOffset;
            return new LagMetricItem(cgo.groupTopicPartition(), lag, cgo.commitTime());
        }).collect(Collectors.toList());
    }

    private void parseLine(String line) {
        Matcher matcher = pattern.matcher(line);
        if (!matcher.find()) {
            return;
        }
        String group = matcher.group(1);
        String topic = matcher.group(2);
        Set<String> groups = topicGroups.get(topic);
        if (groups == null || !groups.contains(group)) {
            return;
        }
        GroupTopicPartition gtp = new GroupTopicPartition(group, topic, Integer.valueOf(matcher.group(3)));
        long offset = Long.valueOf(matcher.group(4));
        long commitTime = Long.valueOf(matcher.group(5));
        ConsumerGroupOffset item = new ConsumerGroupOffset(gtp, offset, commitTime);
        snapshot.put(gtp, item);
    }
}
