package com.hzz.kafka;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.text.ParseException;
import java.util.Properties;

/**
 * Hello world!
 */
public class UserIndicateOffset {
    private static final Logger logger = LoggerFactory.getLogger("searchlog");

    public static void main(String[] args) throws FileNotFoundException, ParseException {
        String group = args[0];
        process(group);
    }

    public static void process(String group) throws ParseException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "node5.secoo-inc.com:6667,node6.secoo-inc.com:6667,node7.secoo-inc.com:6667");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("client.id", "temp-client");
        props.put("auto.offset.reset", "earliest");
        //String group = "hzz-temp-group5";
        props.put("group.id", group);
        long startTime = System.currentTimeMillis();
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        String topic = "mobile_search_product";
//        consumer.subscribe(Lists.newArrayList(topic));
        int partition = Math.abs(group.hashCode()) % 5;
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Lists.newArrayList(topicPartition));
        consumer.seek(topicPartition, 1038000);
//        TopicPartition topicPartition = new TopicPartition(topic, 0);
//        consumer.seek();
        long filterTime = System.currentTimeMillis();
        boolean flag = true;
        while (flag) {
            ConsumerRecords<String, String> records = consumer.poll(10000);
            for (ConsumerRecord<String, String> record : records) {
                //do sth
            }
        }
    }
}
