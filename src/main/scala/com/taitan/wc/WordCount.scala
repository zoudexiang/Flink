package com.taitan.wc

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}

import org.apache.flink.api.scala._

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/5/14 23:15
 *
 **/
object WordCount {

  def main(args: Array[String]): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val dataSet: DataSet[String] = env.readTextFile("D:\\workspace\\flink\\src\\main\\resources\\wc.txt")

    val value: AggregateDataSet[(String, Int)] = dataSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    value.print()
  }
}
