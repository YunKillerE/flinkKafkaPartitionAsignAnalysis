package com.flink.kafkaconsumer;

import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;

public class client {

    public static void main(String[] args) {

/*        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "x.x.x.18:9092,x.x.x.19:9092");
        props.setProperty("group.id", "kk");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("xxxout"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100L);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }*/


        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "x.x.x.18:9092,x.x.x.19:9092");
        props.setProperty("group.id", "kk");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        String topic = "xxxout";
/*        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);
        consumer.assign(Arrays.asList(partition0, partition1));*/
       consumer.subscribe(Arrays.asList("xxxout"));
/*        final int minBatchSize = 200;
        consumer.pause(Collections.singleton(partition0));
        consumer.pause(Collections.singleton(partition1));
        consumer.resume(Collections.singleton(partition0));
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100L);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                System.out.printf("value = %s%n", buffer);
                consumer.commitSync();
                buffer.clear();
            }
        }*/


        //获取topic及其partition
        List<KafkaTopicPartition> partitions = new LinkedList<>();

        for (PartitionInfo partitionInfo : consumer.partitionsFor("xxxout")) {
            System.out.println(partitionInfo.topic() + "-------" + partitionInfo.partition());
            partitions.add(new KafkaTopicPartition(partitionInfo.topic(), partitionInfo.partition()));
        }

        //获取所有的topic
        System.out.println(consumer.listTopics().keySet());

        //根据时间获取offset
        final Map<KafkaTopicPartition, Long> result = new HashMap<>(partitions.size());
        Map<TopicPartition, Long> partitionOffsetsRequest = new HashMap<>(partitions.size());

        for (KafkaTopicPartition partition : partitions) {
            partitionOffsetsRequest.put(
                    new TopicPartition(partition.getTopic(), partition.getPartition()),
                    1559283017L);
        }

        for (Map.Entry<TopicPartition, OffsetAndTimestamp> partitionToOffset :
                consumer.offsetsForTimes(partitionOffsetsRequest).entrySet()) {

            result.put(
                    new KafkaTopicPartition(partitionToOffset.getKey().topic(), partitionToOffset.getKey().partition()),
                    (partitionToOffset.getValue() == null) ? null : partitionToOffset.getValue().offset());
        }

        System.out.println("result ==== "+result);

        //初始化partition 的 offset
        Map<KafkaTopicPartition, Long> subscribedPartitionsToStartOffsets = new HashMap<>();;

        for (KafkaTopicPartition seedPartition : partitions) {
            subscribedPartitionsToStartOffsets.put(seedPartition, -915623761773L);
        }

        System.out.println("subscribedPartitionsToStartOffsets==="+subscribedPartitionsToStartOffsets);


        //获取分配给当前consumer的partition
        final Map<TopicPartition, Long> oldPartitionAssignmentsToPosition = new HashMap<>();
        for (TopicPartition oldPartition : consumer.assignment()) {
            oldPartitionAssignmentsToPosition.put(oldPartition, consumer.position(oldPartition));
        }
        System.out.println("oldPartitionAssignmentsToPosition=="+oldPartitionAssignmentsToPosition);


        System.out.println(consumer.assignment());

        System.out.println("xxxxxxxxxxxxxxxxxxxxxxxx=="+(("topic".hashCode() * 31) & 0x7FFFFFFF) % 4);
        //test
        for (int i = 0; i < 8; i++) {
            Integer startIndex = (("topic".hashCode() * 31) & 0x7FFFFFFF) % 4;
            System.out.println("startIndex=="+startIndex);
            System.out.println("返回值===="+(startIndex + i) % 4);
        }



    }
}
