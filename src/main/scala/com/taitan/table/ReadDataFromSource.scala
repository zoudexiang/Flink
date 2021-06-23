package com.taitan.table

import com.taitan.bean.SensorReading
import com.taitan.utils.GetStreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, Schema}


/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/12/31 10:37
 *
 * third: 从文件和 kafka 读取数据
 *
 **/
object ReadDataFromSource {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = GetStreamExecutionEnvironment.geStreamExecutionEnvironment()
    env.setParallelism(1)

    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    /** 2. 从外部系统读取数据，再环境中注册表 */
    // 2.1 连接到文件系统（csv）
    val filePath = "D:\\workspace\\flink\\src\\main\\resources\\sensor.txt"
    tableEnv.connect(new FileSystem().path(filePath))
      //      .withFormat(new OldCsv()) // 定义读取数据之后的格式化方法（老的方式，已经弃用）
      .withFormat(new Csv()) // 创建 csv 的新方式 需要引入 flink-csv 依赖
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable") // 注册一张表

    // 转换成流打印输出
    val sensorTable: Table = tableEnv.from("inputTable")
    sensorTable.toAppendStream[(String, Long, Double)].print()

    // 2.2 连接到 kafka
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
      .createTemporaryTable("kafkaTable")

    // 转换成流打印输出
    val kafkaTable: Table = tableEnv.from("kafkaTable")
    kafkaTable.toAppendStream[(String, Long, Double)].print()

    env.execute()
  }

}
