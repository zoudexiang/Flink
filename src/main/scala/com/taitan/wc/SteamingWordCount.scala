package com.taitan.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/5/17 17:35
 *
 **/
object SteamingWordCount {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // flink 提供的支持解析参数的工具类, 在该类中需要使用 Run -> Edit Configurations -> 选择 program arguments, 填写 --host localhost --port 7777
    // windows 环境下使用 cmd 输入命令 nc -l -p 7777, 在 linux 环境下使用 nc -lk 7777
    val params: ParameterTool = ParameterTool.fromArgs(args)

    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    val dataStream: DataStream[String] = env.socketTextStream(host, port)

    val resultStream: DataStream[(String, Int)] = dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    resultStream.print()

    env.execute("Flink Streaming Word Count")
  }
}
