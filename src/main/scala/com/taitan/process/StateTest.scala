package com.taitan.process

import java.util
import java.util.concurrent.TimeUnit

import com.taitan.bean.SensorReading
import com.taitan.utils.GetStreamExecutionEnvironment
import org.apache.flink.api.common.functions.{ReduceFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/12/20 20:18
 *
 * 1、几种键控状态（KeyedProcessFunction）的 api 调用
 * 2、几种状态后端
 *     MemoryStateBackend
 *     FsStateBackend
 *     RocksDBStateBackend
 * 3、checkpoint：使用前要声明状态后端
 **/
object StateTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = GetStreamExecutionEnvironment.geStreamExecutionEnvironment()
    env.setParallelism(1)
    /** 声明状态后端 */
//    env.setStateBackend(new MemoryStateBackend())
//    env.setStateBackend(new FsStateBackend(""))
//    env.setStateBackend(new RocksDBStateBackend(""))

    // checkpoint 相关配置，启用检查点，触发检查点的间隔时间（毫秒）
    env.enableCheckpointing(1000L)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置超时时间
    env.getCheckpointConfig.setCheckpointTimeout(30000L)
    // 一个任务可以同时处理的最大的并行检查点 详细查看 day6 checkpoint 配置第十分钟
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    // 设置两个相邻的 checkpoint 之间最小要间隔的时间
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    // 设置任务从 checkpoint 中恢复时是从最近的 checkpoint（true） 中恢复还是 savepoint（false） 中恢复，
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)
    // 设置当前的 checkpoint 最多失败的次数
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)
    // 重启策略的配置
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L))
    env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)))

    val inputDataStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    val sensorDataStream: DataStream[SensorReading] = inputDataStream
      .map (
        data => {
          val arr: Array[String] = data.split(",")
          SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        }
      )

    val warningStream: DataStream[(String, Double, Double)] =
      sensorDataStream.keyBy("id")
//      .flatMap(new MyFlatMapFunction(10.0))
        // 自带状态的 flatMap[R, S](fun: (T, Option[S]) => (TraversableOnce[R], Option[S])) 其中 R 代表输出数据的类型, S 代表状态类型, T 代表输入数据类型
        .flatMapWithState[(String, Double, Double), Double]({
            case (input: SensorReading, None) => (List.empty, Some(input.temperature))
            case (input: SensorReading, lastTemp: Some[Double]) => {
              var diff = (input.temperature - lastTemp.get).abs
              if (diff > 10.0) {
                (List((input.id, lastTemp.get, input.temperature)), Some(input.temperature))
              }  else {
                (List.empty, Some(input.temperature))
              }
            }
          }
        )

    env.execute("state test")
  }
}

/**
 * 使用一种简单的方式实现状态管理 MyFlatMapFunction
 *
 * 可以使用 collector.collect() 的方式将数据写出去
 *
 * @param threshold 阈值
 */
class MyFlatMapFunction(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last_temp", classOf[Double]))

  override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    // 获取保存的状态值以及更新本次的状态值
    val lastTemp = lastTempState.value()
    lastTempState.update(in.temperature)

    val diff: Double = (in.temperature - lastTemp).abs

    if (diff > threshold) {
      collector.collect((in.id, in.temperature, lastTemp))
    }
  }
}

/**
 * 使用一种简单的方式实现状态管理 RichMapFunction
 *
 * 使用 RichMapFunction 来实现一个本次数据与上次数据的温度差值超过阈值进行报警
 *
 * @param threshold 阈值
 */
class MyMapFunction(threshold: Double) extends RichMapFunction[SensorReading, (String, Double, Double)] {

  /** 上一次的温度值 */
  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last_temp", classOf[Double]))
  }

  override def map(in: SensorReading): (String, Double, Double) = {

    // 获取保存的状态值以及更新本次的状态值
    val lastTemp = lastTempState.value()
    lastTempState.update(in.temperature)

    val diff: Double = (in.temperature - lastTemp).abs
    if (diff > threshold) {
      (in.id, in.temperature, lastTemp)
    } else
      (in.id, 0.0, 0.0)
  }
}

/**
 * 几种键控状态的示例
 */
class MyStateProcessor extends KeyedProcessFunction[String, SensorReading, Double] {

  // 两种声明状态的方式，使用 lazy 或者 在生命周期 open() 函数中
  override def open(parameters: Configuration): Unit = {

  }
  /** ValueState */
  lazy val valueState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("value-state", classOf[Double]))

  /** ListState */
  lazy val listState: ListState[String] = getRuntimeContext.getListState(new ListStateDescriptor[String]("list-state", classOf[String]))

  /** MapState */
  lazy val mapState: MapState[String, Double] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Double]("map-state", classOf[String], classOf[Double]))

  /** ReduceState */
  lazy val reduceState: ReducingState[SensorReading] =
    getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading]("reducing-state", new ReduceFunction[SensorReading] {
      override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
        SensorReading(t1.id, t1.timestamp.min(t.timestamp), t1.temperature.max(t.temperature))
      }
    }, classOf[SensorReading]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, Double]#Context, out: Collector[Double]): Unit = {

    // 各种 keyBy 后的状态调用
    /** ValueState */
    val getValueState: Double = valueState.value()
    valueState.update(100.0)
    valueState.clear()

    /** ListState */
    listState.add("add_list_state")
    listState.addAll(new util.ArrayList[String]())
    listState.update(new util.ArrayList[String]())
    listState.clear()

    /** MapState */
    mapState.put("map_state", 100.0)
    mapState.putAll(new util.HashMap[String, Double]())
    mapState.remove("map_state")
    val bool: Boolean = mapState.contains("map_state")
    mapState.get("map_state")
    mapState.keys()
    mapState.clear()

    /** ReduceState */
    reduceState.add(SensorReading("sensor_1", 1234567890, 100))
    reduceState.clear()
  }
}
