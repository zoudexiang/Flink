package com.taitan.windows

import com.taitan.bean.SensorReading
import com.taitan.customize.{FullWindowFunction, MyReduce}
import com.taitan.utils.GetStreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/8/17 21:23
 *
 *  window api 学习
 **/
object WindowTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = GetStreamExecutionEnvironment.geStreamExecutionEnvironment()
    env.setParallelism(1)
    // 设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputDataStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    val sensorDataStream: DataStream[SensorReading] = inputDataStream.map (
      data => {
        val arr: Array[String] = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

    /** window 函数必须要在 keyBy 之后 */
    val resultDataStream: DataStream[SensorReading] = sensorDataStream
      .keyBy("id")
      //.timeWindow(Time.seconds(15))
      .timeWindow(Time.seconds(15), Time.seconds(5))
      .reduce(new MyReduce())
//      .apply(new FullWindowFunction())
       .assignAscendingTimestamps(_.timestamp * 1000L)
    
    resultDataStream.print()

    env.execute()
  }

}
