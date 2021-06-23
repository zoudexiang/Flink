package com.taitan.table

import com.taitan.utils.GetStreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/12/31 10:40
 *
 * forth : 表的查询
 **/
object TableQuery {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = GetStreamExecutionEnvironment.geStreamExecutionEnvironment()
    env.setParallelism(1)

    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

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
