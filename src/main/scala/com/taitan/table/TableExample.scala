package com.taitan.table

import com.taitan.bean.SensorReading
import com.taitan.utils.GetStreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/12/29 16:35
 *
 * first : table api
 **/
object TableExample {

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

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 基于数据流转换为一张表，然后执行操作
    val dataTable: Table = tableEnv.fromDataStream(dataStream)

    // 调用 table api 得到转换结果
    val resultTable: Table = dataTable
      .select("id, temperature")
      .filter("id == 'sensor_1'")

    // 转换回数据流，打印输出
    val tableStream: DataStream[(String, Double)] = resultTable.toAppendStream[(String, Double)]

    tableStream.print("result")
    resultTable.printSchema()

    // 直接写 sql 得到转换结果
    val resultSqlTable: Table = tableEnv.sqlQuery("select id, temperature from " + dataTable + " where id='sensor_1'")
    val tableSqlStream: DataStream[(String, Double)] = resultSqlTable.toAppendStream[(String, Double)]
    tableSqlStream.print("sql result")
    env.execute("table api job")
  }
}
