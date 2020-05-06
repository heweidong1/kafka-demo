package com.kgc.provider;

import com.kgc.partitioner.MyPartitioner;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class SendMessageArsy
{
    public static void main(String[] args) {
        Properties properties =new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        //key 序列化器 key决定消息放入哪个分区
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        //value 序列化器
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        //指定自己的分区管理器
        properties.setProperty("partitioner.class", MyPartitioner.class.getName());
        //kafka生产者对象
        KafkaProducer<String,String> kafkaProducer=new KafkaProducer<String, String>(properties);

        for(int i=0;i<6;i++)
        {
            //发送的消息 发送到哪个主题，key是什么  value是什么
            ProducerRecord<String,String> producerRecord=
                    new ProducerRecord<String, String>("my-topic",null,"test-value"+i);

            //发送消息并且设置异步回调
            kafkaProducer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(null!=exception)
                    {
                        exception.printStackTrace();
                    }
                    if(null!=metadata)
                    {
                        System.out.println(metadata.offset()+"="+metadata.topic());
                    }
                }
            });
        }


        kafkaProducer.flush();
        kafkaProducer.close();
    }

}
