package com.kgc.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class Consumer
{
    public static void main(String[] args) {
        Properties properties =new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        //key 反序列化器 key决定消息放入哪个分区
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        //value 反序列化器
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer<String,String>kafkaConsumer=new KafkaConsumer<String, String>(properties);
        //声明独立消费者，自己制定这个消费者消费哪个主题下的分区,让这个消费者只消费除了2/8分区以外的分区
        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor("my-topic");
        List<TopicPartition> list = new ArrayList<TopicPartition>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            if(partitionInfo.partition()!=2&&partitionInfo.partition()!=8)
            {
                list.add(new TopicPartition("my-topic",partitionInfo.partition()));
            }
        }
        kafkaConsumer.assign(list);
        //拉取消息
        while (true)
        {
            //间隔多久去kafka拉取消息
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(500);
            for(ConsumerRecord<String,String> next : poll )
            {
                System.out.println("消息所在分区："+next.partition()+"消息的偏移量："+next.offset());
                System.out.println("key："+next.key()+"value："+next.value());

            }

        }


    }


}
