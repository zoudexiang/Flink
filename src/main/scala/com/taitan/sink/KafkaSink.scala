package com.taitan.sink

import com.taitan.bean.SensorReading
import com.taitan.utils.{Constants, GetStreamExecutionEnvironment}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/8/16 18:04
 * kafka sink
 **/
object KafkaSink {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = GetStreamExecutionEnvironment.geStreamExecutionEnvironment()
    env.setParallelism(1)

    val textFileDataStream: DataStream[String] = env.readTextFile("D:\\workspace\\flink\\src\\main\\resources\\sensor.txt")

    val resultDataStream: DataStream[String] = textFileDataStream.map(
      data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble).toString
      }
    )

    val sinkTest = "sinkTest"

    resultDataStream.addSink(new FlinkKafkaProducer011[String](Constants.KAFKA_BOOTSTRAP_SERVERS_VALUE, sinkTest, new SimpleStringSchema()))
    env.execute("kafka sink test")
  }
}
