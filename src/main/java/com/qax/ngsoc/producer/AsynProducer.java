package com.qax.ngsoc.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * 异步发送消息
 */
public class AsynProducer {
    public static void main(String[] args) {
        //属性
        Properties properties = new Properties();
        properties.put("bootstrap.server", "192.168.47.102:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //消息
        ProducerRecord<String, String> record = new ProducerRecord<>("qax", "ngsoc");
        try {
            producer.send(record,new KafkaCallBack());
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    static class KafkaCallBack implements Callback{
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if(exception != null){
                exception.printStackTrace();
            }
        }
    }
}
