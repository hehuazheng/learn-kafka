package com.hzz.kafka;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class MyKafkaConsumer {
    public static void main(String[] args) {
        String bootstrapServers = "192.168.33.10:9092";

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("client.id", "test-client");
        props.put("group.id", "test-group");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
        kafkaConsumer.subscribe(Lists.newArrayList("test2"));
        while(true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(30000);
            for (ConsumerRecord<String, String> r : records) {
                String k = r.key();
                String v = r.value();
                System.out.println(k + " # " + v);
            }
        }
    }
}
