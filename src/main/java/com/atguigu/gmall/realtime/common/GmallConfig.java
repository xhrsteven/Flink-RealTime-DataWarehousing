package com.atguigu.gmall.realtime.common;

//项目配置常量类
public class GmallConfig {
    //hbase 命名空间
    public static final String HBASE_SCHEMA = "GMALL2021_REALTIME";
    //phoenix 配置地址
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop01,hadoop02,hadoop03:2181";
}
