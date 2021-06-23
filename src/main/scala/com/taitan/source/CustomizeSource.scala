package com.taitan.source

import com.taitan.bean.SensorReading
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random
import org.apache.flink.api.scala._

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/7/26 21:34
 *
 * 自定义 Source
 **/
class CustomizeSource extends SourceFunction[SensorReading] {

  var running: Boolean = true

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {

    val random = new Random()

    val curTemp = 1.to(10).map(i => ("sensor_"+i, 65 + random.nextGaussian() * 20))

    while (running) {
      val timeStamp: Long = System.currentTimeMillis()

      curTemp.foreach {
        case temp => {
          ctx.collect(SensorReading(temp._1, timeStamp, temp._2))
        }
      }

      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = running = false
}
