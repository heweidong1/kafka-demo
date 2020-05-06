package com.kgc.partitioner;

import org.apache.kafka.common.Cluster;

import java.util.Map;

public class MyPartitioner implements org.apache.kafka.clients.producer.Partitioner {

    public MyPartitioner()
    {

    }

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        System.out.println("使用了我们自己的分区管理器");
        if(keyBytes==null)
        {
            return 0;
        }
        return 1;
    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
