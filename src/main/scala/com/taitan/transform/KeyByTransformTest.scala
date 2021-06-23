package com.taitan.transform

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2021/6/18 21:19
 *
 * 测试 KeyBey 转换算子
 *
 * 测试滚动(Rolling)聚合算子：
 *     min()
 *     max()
 *     sum()
 *     minBy()
 *     maxBy()
 *
 **/
object KeyByTransformTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    var socketDataStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    var tuple2DataStream: DataStream[(String, Int)] = socketDataStream.flatMap(_.split(" ")).map((_, 1))

    val keyByStream: KeyedStream[(String, Int), Tuple] = tuple2DataStream.keyBy(0)

    sumDS.filter()

    var sumDS: DataStream[(String, Int)] = keyByStream.sum(1)
    var minDS: DataStream[(String, Int)] = keyByStream.min(1)
    var maxDS: DataStream[(String, Int)] = keyByStream.max(1)

    // minBy 和 maxBy 与 min 和 max 的区别没整明白
//    keyByStream.minBy()

    sumDS.print("keyBy:")
    minDS.print("min:")
    maxDS.print("max:")

    env.execute()
  }

}
