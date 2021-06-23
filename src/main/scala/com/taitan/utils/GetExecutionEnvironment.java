package com.taitan.utils;


import org.apache.flink.api.scala.ExecutionEnvironment;

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/8/16 18:25
 *
 * 高并发情况下的完美单例模式实现，获取 flink 批模式下的执行环境
 **/
public class GetExecutionEnvironment {

    private static volatile ExecutionEnvironment env = null;

    public static ExecutionEnvironment getExecutionEnvironment() {
        if (env == null) {
            synchronized (GetExecutionEnvironment.class) {
                if (env == null) {
                    env = ExecutionEnvironment.getExecutionEnvironment();
                }
            }
        }
        return env;
    }
}
