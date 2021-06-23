package com.taitan.utils;

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/8/16 18:15
 *
 * 高并发情况下的完美单例模式实现，获取 flink 流模式下的执行环境
 **/
public class GetStreamExecutionEnvironment {

    private static volatile StreamExecutionEnvironment streamEnv = null;

    public static StreamExecutionEnvironment geStreamExecutionEnvironment() {
        if (streamEnv == null) {
            synchronized (GetStreamExecutionEnvironment.class) {
                if (streamEnv == null) {
                    streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
                }
            }
        }
        return streamEnv;
    }
}
