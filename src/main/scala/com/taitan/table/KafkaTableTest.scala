package com.taitan.table

import com.taitan.utils.GetStreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/12/31 15:33
 *
 * 将数据写出到 kafka
 **/
object KafkaTableTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = GetStreamExecutionEnvironment.geStreamExecutionEnvironment()
    env.setParallelism(1)

    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // 从 kafka 读取数据
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("sensor")
      .property("bootstrap-servers", "localhost:9092")
      .property("zookeeper.connect", "localhost:2181")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")

    // 从 kafka 拿数据
    val sensorTable: Table = tableEnv.from("kafkaInputTable")

    val resultTable: Table = sensorTable
      .select('id, 'temperature)
      .filter('id === "sensor_1")

    val groupByResultTable: Table = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'count)

    // 定义一个 kafka 输出表
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("sinkTest")
      .property("bootstrap-servers", "localhost:9092")
      .property("zookeeper.connect", "localhost:2181")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaOutputTable")

    resultTable.insertInto("kafkaOutputTable")
    env.execute()
  }
}
