package com.atguigu.gmall.realtime.bean;

//配置表对应的实体类

import lombok.Data;

@Data
public class TableProcess {
    public static final String SINK_TYPE_HBASE="hbase";
    public static final String SINK_TYPE_KAFKA="kafka";
    public static final String SINK_TYPE_CLICKHOUSE="clickhouse";

    String sourceTable;

    String operateType;

    String sinkType;

    String sinkTable;

    String sinkColumns;

    String sinkPk;

    String sinkExtend;

}
