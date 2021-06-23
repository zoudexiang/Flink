package com.taitan.source

import com.taitan.bean.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/7/26 20:47
 *
 * 从集合读取数据
 **/
object ReadSourceFromCollection {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val collectionDataStream: DataStream[SensorReading] = env.fromCollection(
      List(
        SensorReading("sensor_1", 1547718199, 35.8),
        SensorReading("sensor_6", 1547718201, 15.4),
        SensorReading("sensor_7", 1547718202, 6.7),
        SensorReading("sensor_10", 1547718205, 38.1)
      )
    )

    collectionDataStream.print("从集合读取数据：").setParallelism(1)

    env.execute()
  }

}
