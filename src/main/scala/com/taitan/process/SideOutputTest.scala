package com.taitan.process

import com.taitan.bean.SensorReading
import com.taitan.utils.GetStreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/12/20 14:34
 *
 * 使用 process function 来进行侧输出流的操作
 * 案例：区分高温和低温
 **/
object SideOutputTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = GetStreamExecutionEnvironment.geStreamExecutionEnvironment()
    env.setParallelism(1)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    env.getConfig.setAutoWatermarkInterval(500L) // 设置自动生成 watermark 的时间间隔

    val inputDataStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    val sensorDataStream: DataStream[SensorReading] = inputDataStream
      .map (
        data => {
          val arr: Array[String] = data.split(",")
          SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        }
      )

    val highTempStream: DataStream[SensorReading] = sensorDataStream.process(new SplitTempProcessor(30.0))

    val lowerTempStream: DataStream[(String, Double, Long)] = highTempStream.getSideOutput(new OutputTag[(String, Double, Long)]("lower-high"))

    highTempStream.print("high temp")
    lowerTempStream.print("lower temp")

    env.execute("split temp job")

  }

}

/**
 * 自定义 process function
 * @param threshold 区分高低温的界值
 */
class SplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading, SensorReading] {

  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {

    /** 判断当前温度值是否大于阈值，大于输出到主流，小于输出到侧输出流 */
    if (value.temperature > threshold) {
      out.collect(value)
    } else {
      ctx.output(new OutputTag[(String, Long, Double)]("lower-high"), (value.id, value.timestamp, value.temperature))
    }
  }
}