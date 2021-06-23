package com.taitan.transform

import com.taitan.bean.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2021/6/18 22:44
 *
 *      split 算子会将数据拆成不同的 SplitStream
 *
 *      select 算子通过传入 split 算子计算时的 name 可以拿到对应 SplitStream 的数据
 *
 *      在流式处理中貌似不好使, 因为数据来一条处理一条, 没有啥意义, 就像本代码一样, 测试不出实际的意义
 *
 *      建议使用 StreamExecutionEnvironment.getExecutionEnvironment.fromCollection() 获得一批流式数据,
 *      而不是像本案例一次计算获得的只是一条数据(Flink 流式处理本质), 可以测试出实际意义
 *
 **/
object SplitAndSelectTransform {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    var socketDataStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    val sensorReadingDS: DataStream[SensorReading] = socketDataStream.map(
      line => {
        val arr: Array[String] = line.split(" ")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

    var splitDS: SplitStream[SensorReading] = sensorReadingDS.split(
      sensorReading => {
        if (sensorReading.temperature > 30) {
          Seq("high")
        } else {
          Seq("low")
        }
      }
    )

//    splitDS.print()

    val high: DataStream[SensorReading] = splitDS.select("high")
    val low: DataStream[SensorReading] = splitDS.select("low")
    val all: DataStream[SensorReading] = splitDS.select("high", "low")

    high.print("high")
    low.print("low")
    all.print("all")
    env.execute()
  }

}
