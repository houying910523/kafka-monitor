package com.ke.bigdata.streaming.kafka.monitor.util;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * author: hy
 * date: 2019/2/9
 * desc:
 */
public class JsonUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String toJsonString(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (IOException e) {
            return null;
        }
    }
}
