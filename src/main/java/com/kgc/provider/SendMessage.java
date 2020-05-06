package com.kgc.provider;

import com.kgc.partitioner.MyPartitioner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SendMessage
{
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties =new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        //key 序列化器 key决定消息放入哪个分区
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        //value 序列化器
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        //指定自己的分区管理器
        properties.setProperty("partitioner.class", MyPartitioner.class.getName());
        //批量发送，多久发送一次详细，默认是0秒，不缓存，通常和batch.size配合使用
        //batch.size当本地缓存中数据到达一定的大小的时候，统一发送到kafka中
        properties.setProperty("linger.ms", "20000");


        //kafka生产者对象
        KafkaProducer<String,String> kafkaProducer=new KafkaProducer<String, String>(properties);

        //发送的消息 发送到哪个主题，key是什么  value是什么
        ProducerRecord<String,String> producerRecord=
                new ProducerRecord<String, String>("my-topic",null,"test-value");

        //判断是否发送成功，当发送成功会返回这个消息的数据，这是同步的
        Future<RecordMetadata> send = kafkaProducer.send(producerRecord);
        RecordMetadata recordMetadata = send.get();
        System.out.println(recordMetadata.offset()+recordMetadata.topic());

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
