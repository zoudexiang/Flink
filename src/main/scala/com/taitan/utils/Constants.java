package com.taitan.utils;

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved
 *
 * @Author dexiang.zou
 * @Date 2020/7/26 21:25
 *
 * 常量类
 **/
public class Constants {

    public static String KAFKA_BOOTSTRAP_SERVERS_KEY = "bootstrap.servers";
    public static String KAFKA_BOOTSTRAP_SERVERS_VALUE = "hadoop201:9092,hadoop202:9092,hadoop203:9092";

    public static String KAFKA_GROUP_ID_KEY = "group_id";

    public static String KAFKA_KEY_DESERIALIZER = "key.deserializer";
    public static String KAFKA_VALUE_DESERIALIZER = "value.deserializer";

    public static String KAFKA_AUTO_OFFSET_RESET = "auto.offset.reset";

}
