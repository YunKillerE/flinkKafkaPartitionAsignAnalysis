package com.flink.kafkaconsumer

import java.util
import java.util.{HashMap, Map, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

object consumer {

  def main(args: Array[String]): Unit = {

    val props = new Properties
    props.setProperty("bootstrap.servers", "x.x.x.18:9092,x.x.x.19:9092")
    props.setProperty("group.id", "kk")
    props.setProperty("enable.auto.commit", "false")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val consumer = new KafkaConsumer[String, String](props)

    val topic = "xxxout"

    consumer.subscribe(util.Arrays.asList("xxxout"))

    import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
    //获取topic及其partition
    val partitions = new util.LinkedList[KafkaTopicPartition]()

    import scala.collection.JavaConversions._
    for (partitionInfo <- consumer.partitionsFor("xxxout")) {
      System.out.println(partitionInfo.topic + "-------" + partitionInfo.partition)
      partitions.add(new KafkaTopicPartition(partitionInfo.topic, partitionInfo.partition))
    }


    //初始化partition 的 offset
    val subscribedPartitionsToStartOffsets = new util.HashMap[KafkaTopicPartition, Long]

    import scala.collection.JavaConversions._
    for (seedPartition <- partitions) {
      subscribedPartitionsToStartOffsets.put(seedPartition, -915623761773L)
    }

    System.out.println("subscribedPartitionsToStartOffsets===" + subscribedPartitionsToStartOffsets)

    Thread.sleep(30000)

    //测试spark的动态分区
    System.out.println("测试spark的动态分区===")

    val c = consumer
    c.poll(0)

    val partitions1 = new util.LinkedList[KafkaTopicPartition]()

    import scala.collection.JavaConversions._
    for (partitionInfo <- c.partitionsFor("xxxout")) {
      System.out.println(partitionInfo.topic + "-------" + partitionInfo.partition)
      partitions1.add(new KafkaTopicPartition(partitionInfo.topic, partitionInfo.partition))
    }

    val subscribedPartitionsToStartOffsets1 = new util.HashMap[KafkaTopicPartition, Long]

    import scala.collection.JavaConversions._
    for (seedPartition <- partitions1) {
      subscribedPartitionsToStartOffsets1.put(seedPartition, -915623761773L)
    }

    System.out.println("subscribedPartitionsToStartOffsets===" + subscribedPartitionsToStartOffsets1)

  }

}

