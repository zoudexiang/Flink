package com.taitan.transform

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2021/6/18 22:19
 *
 *  reduce() 算子是将 KeyedStream 合并成 DataStream 的过程
 *  合并当前的元素和上次聚合的结果，产生一个新的值，返回的流中包含每一次聚合的结果，而不是只返回最后一次聚合的最终结果
 *  这里的返回每一次的聚合结果指的是本次计算和上次聚合的结果，不是将所有的历史聚合结果都输出，只输出本次计算的结果
 **/
object ReduceTransform {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    var socketDataStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    var tuple2DataStream: DataStream[(String, Int)] = socketDataStream.flatMap(_.split(" ")).map((_, 1))

    val keyByStream: KeyedStream[(String, Int), Tuple] = tuple2DataStream.keyBy(0)

    val resultDS: DataStream[(String, Int)] = keyByStream.reduce((x, y) => (x._1, y._2 + x._2))

    resultDS.print()

    env.execute()
  }

}
