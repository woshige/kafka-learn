package com.qax.ngsoc.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Properties;

/**
 * 简单的kafka生产者
 */
public class SimpleProducer {
    public static void main(String[] args) {
        //属性
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.47.102:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //消息
        ProducerRecord<String, String> record = new ProducerRecord<>("qax", "ngsoc");
        try {
            for (int i = 0; i < 100; i++) {
                System.out.println("------send---------");
                producer.send(record).get();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
