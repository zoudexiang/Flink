package com.taitan.sink

import java.util

import com.taitan.bean.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/10/30 13:44
 *
 **/
object FlinkElasticsearchSink {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(6000)

    var dataStream: DataStream[String] = env.readTextFile("src\\main\\resources\\wc.txt")
    val resultDataStream: DataStream[SensorReading] = dataStream.map(
      data => {
        val dataArr: Array[String] = data.split(",")
        SensorReading(dataArr(0), dataArr(1).toLong, dataArr(2).toDouble)
      }
    )

//    new util.ArrayList[HttpHost]()
  }

}
