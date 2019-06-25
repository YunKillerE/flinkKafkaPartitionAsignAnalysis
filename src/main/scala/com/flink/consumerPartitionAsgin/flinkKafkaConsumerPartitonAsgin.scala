package com.flink.consumerPartitionAsgin

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object flinkKafkaConsumerPartitonAsgin {

  private val ZOOKEEPER_HOST = "x.x.x.253:2181,x.x.x.254:2181,x.x.x.255:2181/kafkas"
  private val KAFKA_BROKER = "x.x.x.18:9092,x.x.x.19:9092"
  private val GROUP_ID = "partitiontest"

  def main(args: Array[String]) {

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // configure Kafka consumer
    val kafkaProps = new Properties
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", "xxxxxxx")
    // always read the Kafka topic from the start
    kafkaProps.setProperty("auto.offset.reset", "earliest")
    kafkaProps.setProperty("flink.partition-discovery.interval-millis","1000")

    //streing deserialize
    val consumer = new FlinkKafkaConsumerY[String](
      args(0), new SimpleStringSchema(), kafkaProps)

    // create a Kafka source
    //import org.apache.flink.api.scala._
    implicit val typeInfo = TypeInformation.of(classOf[(String)])
    val ycSource = env.addSource(consumer).setParallelism(4)

    //transformations
    ycSource.print()

    // execute the transformation pipeline
    env.execute("Comsumer Partitoin Asgin Test")
  }

}
