package gooyuanly.io.kafka.monitor.util;

import com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Map;

/**
 * author: hy
 * date: 2019/2/19
 * desc:
 */
public class KafkaUtils {

    public static Map<String, Object> consumerConfig(String bootstrap, String group) {
        Map<String, Object> params = Maps.newHashMap();
        params.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        params.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        params.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        params.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        params.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        params.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        params.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000");
        return params;
    }
}
