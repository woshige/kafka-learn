package com.qax.ngsoc.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class kafkaConsumerExam {
    private static Logger logger = LoggerFactory.getLogger(kafkaConsumerExam.class);

    public static void main(String[] args) {//属性
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.47.102:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "qax_group_04");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("qax"));
        Duration duration = Duration.ofMillis(1000);
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(duration);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("------------------");
                    logger.info("partition{},topic{},record{}", record.partition(), record.topic(), record.value());
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
