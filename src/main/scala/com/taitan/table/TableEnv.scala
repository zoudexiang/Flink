package com.taitan.table

import com.taitan.bean.SensorReading
import com.taitan.utils.GetStreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.scala._

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/12/31 10:34
 *
 * second : 四种 table env 的介绍
 *
 **/
object TableEnv {

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
  }
}
