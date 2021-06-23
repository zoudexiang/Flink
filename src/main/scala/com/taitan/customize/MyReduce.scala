package com.taitan.customize

import com.taitan.bean.SensorReading
import org.apache.flink.api.common.functions.ReduceFunction

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/8/17 21:52
 *
 * 自定义 reduce function
 **/
class MyReduce() extends ReduceFunction[SensorReading]{

  /**
   *
   * @param t 前一次计算的结果
   * @param t1 输入进来需要计算的数据
   * @return 前一次计算的结果和这次输入进来的数据计算出得新结果
   */
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
    SensorReading(t.id, t.timestamp.max(t1.timestamp), t.temperature.min(t1.temperature))
  }
}
