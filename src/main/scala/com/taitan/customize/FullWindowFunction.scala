package com.taitan.customize

import com.taitan.bean.SensorReading
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/8/17 22:48
 * 全窗口测试类
 **/
class FullWindowFunction extends WindowFunction[SensorReading, (Long, Int), Tuple, TimeWindow]{

  /**
   *
   * @param key
   * @param window
   * @param input
   * @param out 通过调用 out 的 collect 方法可以将数据写出去
   */
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[SensorReading], out: Collector[(Long, Int)]): Unit = {
    out.collect((window.getStart, input.size))
  }
}
