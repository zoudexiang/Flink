package com.taitan.source

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}

import org.apache.flink.api.scala._
/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/7/26 20:57
 *
 **/
object ReadSourceFromTextFile {

  def main(args: Array[String]): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val textFileDataSet: DataSet[String] = env.readTextFile("D:\\workspace\\flink\\src\\main\\resources\\wc.txt")

    val aggDataSet: AggregateDataSet[(String, Int)] = textFileDataSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    aggDataSet.print()
  }

}
