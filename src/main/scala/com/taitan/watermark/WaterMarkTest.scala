package com.taitan.watermark

import com.taitan.bean.SensorReading
import com.taitan.customize.MyReduce
import com.taitan.utils.GetStreamExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/12/12 23:41
 *
 * watermark 入门
 *
 * 使用了 watermark 的开窗，关窗的要求是“当前窗口最大的时间戳 - watermark = 窗口关闭时间戳”
 **/
class WaterMarkTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = GetStreamExecutionEnvironment.geStreamExecutionEnvironment()
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.getConfig.setAutoWatermarkInterval(500L) // 设置自动生成 watermark 的时间间隔

    val inputDataStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    val sensorDataStream: DataStream[SensorReading] = inputDataStream
      .map (
        data => {
          val arr: Array[String] = data.split(",")
          SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        }
    )

    /** Time.seconds(1) 就是最大乱序时间 */
    val waterMarkDataStream: DataStream[SensorReading] = sensorDataStream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
    }
      // new MyPeriodicWMAssigner(Time.seconds(1))
      // new MyPunctuatedWMAssigner(Time.seconds(1))
    )

    /** 处理乱序数据的三步骤：watermark + window 延迟 + 侧输出流 */
    val resultDataStream: DataStream[SensorReading] = waterMarkDataStream
      .keyBy("id")
      .timeWindow(Time.seconds(15), Time.seconds(5))
      // waterMarkDataStream 已经使用了延迟一秒，若此时使用了 allowLateness , 则表示
      //     watermark 在延迟一秒的时候会先触发一次计算输出一次结果但是窗口不关闭，
      //     等到在延迟第 61 秒的时候才进行窗口关闭。
      //     这样做的目的是为了将在 watermark 延迟一秒之外的数据也进行计算
      //     而且这 60 秒迟到的数据不是放到之前的桶中, 而是和 watermark 迟到的数据进行一次计算, 而且是来一条输出一次结果
      .allowedLateness(Time.seconds(60))
      // 侧输出流: 如果前面 watermark + window 的组合还有迟到的数据，则将这部分数据放入侧输出流
      .sideOutputLateData(new OutputTag[SensorReading]("late"))
      .reduce(new MyReduce())
//      .apply(new MyReduce())

    // 将侧输出流中的数据拿出来
    val resultSideDataStream: DataStream[SensorReading] = resultDataStream.getSideOutput(new OutputTag[SensorReading]("late"))

    // TODO resultDataStream + resultSideDataStream 的后续聚合处理

    waterMarkDataStream.print("data")
    resultDataStream.print("result")
    env.execute("WaterMark Test")

  }

}

/**
 * 自定义一个周期性的 WaterMark
 *
 * 一般情况下  都是周期性的 WaterMark ，因为实际生产环境下数据量很密集  如果采用断点式的 WaterMark 每来一条数据就要生成一个 WaterMark 会影响 Flink 效率
 *
 * 该案例是对照上面 BoundedOutOfOrdernessTimestampExtractor 实现而来的
 * @param lateness 延迟时间
 */
class MyPeriodicWMAssigner(lateness: Long) extends AssignerWithPeriodicWatermarks[SensorReading] {

  var maxTs: Long = Long.MinValue + lateness

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - lateness)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {

    maxTs = maxTs.max(element.timestamp * 1000L)
    element.timestamp * 1000L
  }
}

/**
 * 自定义一个断点式的 WaterMark
 *
 * @param lateness 延迟时间
 */
class MyPunctuatedWMAssigner(lateness: Long) extends  AssignerWithPunctuatedWatermarks[SensorReading] {

  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {

    if (lastElement.id == "sensor_1") {
      new Watermark(extractedTimestamp - lateness)
    } else {
      null
    }
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = element.timestamp * 1000L
}
