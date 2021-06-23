package com.taitan.transform

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2021/6/18 23:05
 *
 * connect() 算子
 *
 *     DataStream,DataStream → ConnectedStreams：
 *       连接两个保持他们类型的数据流，两个数据流被 Connect 之后，只是被放在了一个同一个流中，内部依然保持各自的数据和形式不发生任何变化，两个流相互独立
 *
 *     ConnectedStreams → DataStream：
 *       作用于 ConnectedStreams 上，功能与 map 和 flatMap 一样，对 ConnectedStreams 中的每一个 Stream 分别进行 map 和 flatMap 处理
 *
 * union() 算子
 *
 *     var newDS = DS1.union(DS2)
 *
 *     对两个或者两个以上的 DataStream 进行 union 操作，产生一个包含所有 DataStream 元素的新 DataStream
 *
 * connect 和 union 的区别：
 *     1、Union 之前两个流的类型必须是一样, Connect 可以不一样，在之后的 coMap 中再去调整成为一样的
 *     2、Connect 只能操作两个流，Union 可以操作多个
 **/
object ConnectAndUnionTransform {

  def main(args: Array[String]): Unit = {


  }

}
