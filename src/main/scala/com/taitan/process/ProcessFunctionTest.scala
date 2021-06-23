package com.taitan.process

import com.taitan.bean.SensorReading
import com.taitan.utils.GetStreamExecutionEnvironment
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * process function 的学习
 *
 * @Author dexiang.zou
 * @Date 2020/12/19 19:43
 *
 **/
object ProcessFunctionTest {

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

//    val value: KeyedStream[SensorReading, Tuple] = sensorDataStream
//      .keyBy("id")
    /** 检测每一个传感器温度是否连续上升，在 10 s 之内 */
    val warningDataStream: DataStream[String] = sensorDataStream
      .keyBy("id")
      .process(new MyKeyedProcessFunction(10000L))

    warningDataStream.print()
    env.execute("temp_incr_warning")

  }

}

/**
 * 转换算子是无法访问事件的时间戳信息和水位线信息的
 *
 * ProcessFunction 可以访问时间戳、watermark 以及注册定时事件。还可以输出特定的一些事件，例如超时事件等
 * 使用 process function 演示定时器，超过 10 s 温度没有下降则报警
 *
 * @param interval
 */
class MyKeyedProcessFunction(interval: Long) extends KeyedProcessFunction[Tuple, SensorReading, String] {

  // 温度保留状态：由于需要跟之前的温度值做对比，所以将上一个温度保存成状态
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  // 时间戳保留状态：为了方便删除定时器。还需要保存定时器的时间戳
  lazy val curTimerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("cur-timer-ts", classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {

    // 首先取出状态
    val lastTemp = lastTempState.value()
    val curTimerTs = curTimerTsState.value()

    lastTempState.update(value.temperature)

    // 判断当前温度值，如果比之前温度高，并且没有定时器的话，注册 10 s 后的定时器
    if (value.temperature > lastTemp && curTimerTs == 0) {

      val ts: Long = ctx.timerService().currentProcessingTime() + interval

      ctx.timerService().registerProcessingTimeTimer(ts)
      curTimerTsState.update(ts)
    } else if (value.temperature < lastTemp) {
      // 温度下降，删除定时器
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      curTimerTsState.clear()
    }
    // 如果上面的逻辑都不满足，且到了触发定时器报警时间，则使用 onTimer 进行定时器对应的逻辑处理
  }

  /**
   * 定时器触发，10 s 内温度没有下降，报警
   * @param timestamp
   * @param ctx
   * @param out
   */
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("温度值连续 " + interval/1000 + " 秒内没有下降...")
    curTimerTsState.clear()
  }

}
