package com.taitan.sink

import com.taitan.bean.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/10/30 13:27
 *
 * flink redis sink connnector
 *
 **/
object FlinkRedisSink {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(6000)

    val dataStream: DataStream[String] = env.readTextFile("src\\main\\resources\\wc.txt")

    val resultDataStream: DataStream[SensorReading] = dataStream.map(
      data => {
        val arr: Array[String] = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("").setPort(6379).build()
    resultDataStream.addSink(new RedisSink[SensorReading](conf, new FlinkRedisMapper))


    env.execute("flink_redis_sink")
  }
}


class FlinkRedisMapper extends RedisMapper[SensorReading] {

  override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
  }

  override def getKeyFromData(t: SensorReading): String = {
    t.id.toString
  }

  override def getValueFromData(t: SensorReading): String = {
      t.temperature.toString
  }
}