package com.ke.bigdata.streaming.kafka.monitor;

import com.ke.bigdata.streaming.kafka.monitor.kafka.KafkaCluster;

/**
 * author: hy
 * date: 2019/2/19
 * desc:
 */
public class Test {

    public static void main(String[] args) throws Exception {
        String zk = args[0];
        String path = args[1];

        KafkaCluster kc = new KafkaCluster(zk, path);

        kc.listGroups(args[2]).forEach(System.out::println);
    }
}
