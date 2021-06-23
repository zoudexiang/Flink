package com.taitan.table

import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import com.taitan.utils.GetStreamExecutionEnvironment
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, OldCsv, Schema}

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/12/29 17:10
 *
 *  1、四种 table env 的介绍
 *
 *  2、从外部文件系统读数据
 *
 *  3、从 kafka 读数据
 **/
object TableApiTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = GetStreamExecutionEnvironment.geStreamExecutionEnvironment()

    /** 1. 创建表环境 */
    // 1.1 创建老版本的流查询环境
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // 1.2  创建老版本的批式查询环境
    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEnv: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)

    // 1.3 创建 blink 版本的流查询环境
    val blinkStreamSettings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val blinkStreamEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, blinkStreamSettings)

    // 1.4 创建 blink 版本的批式查询环境 需要指定 -target:jvm-1.8
//    val blinkBatchSettings: EnvironmentSettings = EnvironmentSettings
//      .newInstance()
//      .useBlinkPlanner()
//      .inBatchMode()
//      .build()
//    val blinkBatchTableEnv: TableEnvironment = TableEnvironment.create(blinkBatchSettings)

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

    /** 3. 表的查询 */
    // 3.1 简单查询，过滤投影
//    val sensorTable: Table = tableEnv.from("inputTable")
    val resultTable: Table = sensorTable
      .select('id, 'temperature)
      .filter('id === "sensor_1")
    resultTable.toAppendStream[(String, Double)].print("resultTable")

    // 3.2 SQL 简单查询
    val resultSQLTable: Table = tableEnv.sqlQuery(
      """
        |select id, temperature
        |from inputTable
        |where id='sensor_1'
        |""".stripMargin
    )
    // 此处不是追加流，因为进行聚合了，所以使用 toRetractStream
    resultSQLTable.toRetractStream[(String, Double)].print("resultSQLTable")

    // 3.3 简单聚合, 统计每个传感器温度个数
    val groupByResultTable: Table = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'count)
    groupByResultTable.toRetractStream[(String, Long) ].print("groupByResultTable")

    // 3.4 SQL 实现简单聚合
    val groupBySQLRestultTable: Table = tableEnv.sqlQuery(
      """
        |select id, count(id) as cnt
        |from inputTable
        |group by id
        |""".stripMargin)


    env.execute("table api test job")
  }
}
