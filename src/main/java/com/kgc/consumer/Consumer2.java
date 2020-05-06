package com.kgc.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class Consumer2
{
    private static Long index=0L;

    public static void main(String[] args) {
        Properties properties =new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        //key 反序列化器 key决定消息放入哪个分区
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        //value 反序列化器
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        //手动提交偏移量
        properties.setProperty("enable.auto.commit", "false");
        //定义消费者群组
        properties.setProperty("group.id","1112");

        final KafkaConsumer<String,String>kafkaConsumer=new KafkaConsumer<String, String>(properties);
        //定义一个主题
        kafkaConsumer.subscribe(Collections.singletonList("my-topic"), new ConsumerRebalanceListener() {
            //分区再均衡之前调用
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("分区再均衡之前");
                kafkaConsumer.commitSync();
            }
            //分区再均衡之后调用
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("分区再均衡之后");
                for (TopicPartition partition : partitions) {
                    System.out.println("新分配到的分区："+partition.partition());
                }
            }
        });


        Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<TopicPartition, OffsetAndMetadata>();



        try {
            //拉取消息
            while (true)
            {
                //间隔多久去kafka拉取消息
                ConsumerRecords<String, String> poll = kafkaConsumer.poll(500);
                for(ConsumerRecord<String,String> next : poll )
                {
                    System.out.println("消息所在分区："+next.partition()+"消息的偏移量："+next.offset());
                    System.out.println("key："+next.key()+"value："+next.value());
                    index=next.offset();

                }
                //我这里就指定了test-topic这个主题下的分区1 OffsetAndMetadata:第一个参数为你要提交的偏移量 第二个
                //参数可以选择性的传入业务ID 可以拿来确定这次提交 这里我直接提交偏移量为0 那么会导致下个消费者或者说分区
                //再均衡之后再来读取这个分区的数据会从第一条开始读取

                //offset.put(new TopicPartition("my-topic", 0), new OffsetAndMetadata(index+1, "1"));
                //指定偏移量提交 参数为map集合 key为指定的主题下的分区，value 为你要提交的偏移量

                //kafkaConsumer.commitSync(offset);

                //异步提交偏移量，当有一次提交出现错误，也没关系，当最新的提交提交成功，就行
                kafkaConsumer.commitAsync();
            }
        }catch (Exception e)
        {
            e.printStackTrace();
        }finally {
            try {
                //因为异步提交，因为有下一次，所以当其中有一次出现错误，也没事，只要是最新的偏移量
                //提交成功就可以，但是当虚拟机出现问题，这个消费者暴毙，就不会有下一次，所以要用同步提交
                //同步提交偏移量
                kafkaConsumer.commitSync();
            }catch (Exception e)
            {
                e.printStackTrace();
            }finally {
                kafkaConsumer.close();
            }

        }



    }


}
