package com.taitan.table

import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import com.taitan.bean.SensorReading
import com.taitan.utils.GetStreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/12/31 10:47
 *
 * fifth : 流转换成 表 、view
 *
 *         将数据写入文件
 **/
object OutputTable {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = GetStreamExecutionEnvironment.geStreamExecutionEnvironment()
    env.setParallelism(1)

    val inputDataStream: DataStream[String] = env.readTextFile("D:\\workspace\\flink\\src\\main\\resources\\sensor.txt")

    val dataStream: DataStream[SensorReading] = inputDataStream
      .map (
        data => {
          val arr: Array[String] = data.split(",")
          SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        }
      )

    // 创建表环境
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)


    // 将 DataStream 转换成 table
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature as 'temp, 'timestamp as 'ts)

    //*******************************************************************************
    // 将 DataStream 转换成 view 的 2 种方式
//    tableEnv.createTemporaryView("sensorView", dataStream)
//    tableEnv.createTemporaryView("sensorView", dataStream, 'id, 'temperature as 'temp, 'timestamp as 'ts)

    // 将 table 转换成 view
//    tableEnv.createTemporaryView("sensorView", sensorTable)
    //*******************************************************************************

    // 对数据进行操作，得到一个结果表
    val resultTable: Table = sensorTable
      .select('id, 'temp)
      .filter('id === "sensor_1")

    val groupByResultTable: Table = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'count)

    tableEnv.connect(new FileSystem().path("D:\\workspace\\flink\\src\\main\\resources\\file_result.txt"))
        .withFormat(new Csv())
        .withSchema(new Schema()
          .field("id", DataTypes.STRING())
          .field("temp", DataTypes.DOUBLE())
        )
        .createTemporaryTable("outputTable")

    // 将结果表写出 必须是没有更新的操作
    resultTable.insertInto("outputTable")

//    sensorTable.printSchema()
    sensorTable.toAppendStream[(String, Double, Long)].print()
    env.execute("output table test")
  }

}
