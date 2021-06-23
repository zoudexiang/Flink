package com.taitan.source

import java.util.Properties

import com.taitan.utils.Constants
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.scala._

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/7/26 21:16
 *
 * 从 kafka 读取数据
 **/
object ReadSourceFromKafka {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()

    properties.setProperty(Constants.KAFKA_BOOTSTRAP_SERVERS_KEY, Constants.KAFKA_BOOTSTRAP_SERVERS_VALUE)
    properties.setProperty(Constants.KAFKA_GROUP_ID_KEY, "consumer-group")
    properties.setProperty(Constants.KAFKA_KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty(Constants.KAFKA_VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty(Constants.KAFKA_AUTO_OFFSET_RESET, "latest")

    val kafkaDataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    kafkaDataStream.print()

    env.execute()
  }

}
