package com.taitan.table

import java.sql.Timestamp

import com.taitan.bean.SensorReading
import com.taitan.utils.GetStreamExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Over, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2021/7/11 16:36
 *
 **/
object TableWindowTest {

  def main(args: Array[String]): Unit = {
    // 1、创建执行环境和时间语义
    val env: StreamExecutionEnvironment = GetStreamExecutionEnvironment.geStreamExecutionEnvironment()
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2、读取数据创建 DataStream
    val inputStream: DataStream[String] = env.readTextFile("D:\\workspace\\flink\\src\\main\\resources\\sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStream.map(
      data => {
        val dataArr: Array[String] = data.split(",")
        SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
      }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(element: SensorReading) = element.timestamp * 1000L
    })

    // 3、创建表环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)


    // 4、将 DataStream 转换成 Table
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp.rowtime as 'ts, 'temperature)
    tableEnv.createTemporaryView("sensor", sensorTable)

    // 5、窗口操作: table-api 和 flink-sql 的实现方式
    groupWindowTest(tableEnv, sensorTable)

    // 6、over 操作: table-api 和 flink-sql 的实现方式
    overWindowTest(tableEnv, sensorTable)

    env.execute("time and window test!")
  }

  /**
   * 窗口操作：table-api 和 flink-sql 的实现方式
   * @param tableEnv
   * @param sensorTable
   */
  def groupWindowTest(tableEnv: StreamTableEnvironment, sensorTable: Table): Unit = {

    // 5、窗口操作(Table API) - 滚动窗口
    val groupResultTableAPITable: Table = sensorTable.window(Tumble over 10.seconds on 'ts as 'tw)
      .groupBy('id, 'tw)
      .select('id, 'id.count, 'tw.end)

    groupResultTableAPITable.toRetractStream[(String, Long, Timestamp)].print("table api group result")

    // 6、窗口操作(Flink SQL) - 滚动窗口
    val groupResultFlinkSQLTable: Table = tableEnv.sqlQuery(
      """
        |
        |select
        |    id,
        |    count(id),
        |    tumble_end(ts, interval '10' second)
        |from sensor
        |group by id,
        |    tumble(ts, interval '10' second)
        |""".stripMargin)

    groupResultFlinkSQLTable.toAppendStream[(String, Long, Timestamp)].print("flink sql group result")
  }

  /**
   * over 操作：table-api 和 flink-sql 的实现方式
   * @param tableEnv
   * @param sensorTable
   * @return
   */
  def overWindowTest(tableEnv: StreamTableEnvironment, sensorTable: Table) = {

    // table api - over 操作
    val overResultAPITable: Table = sensorTable
      .window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'w)
      .select('id, 'ts, 'id.count over 'w, 'temperature.avg over 'w)

    // flink sql - over 操作
    val overResultSQLTable: Table = tableEnv.sqlQuery(
      """
        |select
        |    id,
        |    count(id) over w,
        |    avg(temperature) over w
        |from sensor
        |window w as (
        |    partition by id
        |    order by ts
        |    rows between 2 preceding and current row
        |)
        |""".stripMargin)

    overResultAPITable.toAppendStream[Row].print("table api over result")
    overResultSQLTable.toAppendStream[Row].print("flink sql over result")
  }

}
