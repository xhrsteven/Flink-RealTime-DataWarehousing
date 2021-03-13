package com.atguigu.gmall.realtime.app.dwd;
//准备业务数据的DWD层数据


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class BaseDBApp {
    public static void main(String[] args) throws Exception{
        //环境准备
        //1.创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2设置并行度--kafka
        env.setParallelism(1);

        //ck 精准一次性
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/flink/checkpoint/baselogApp"));

        //重启策略 --会尝试重启
//        env.setRestartStrategy(RestartStrategies.noRestart());

        //从Kafka的ODS层读取数据
        String topic ="ods_base_db_m";
        String groupId = "base_db_app_group";

        //2.1通过工具类获取Kafka消费者
        FlinkKafkaConsumer<String> kafkasource = MyKafkaUtil.getKafkasource(topic, groupId);

        DataStreamSource<String> jsonStrDS = env.addSource(kafkasource);

//        jsonStrDS.print("json>>>>>>>>");
        //对DS中数据进行结构转换 String -> Json
//        jsonStrDS.map(new MapFunction<String, JSONObject>() {
//            @Override
//            public JSONObject map(String value) throws Exception {
//                JSONObject.parseObject(value)
//                return null;
//            }
//        })

        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(
                jsonStr -> JSON.parseObject(jsonStr)
        );


//        jsonStrDS.map(JSON::parseObject);

        //3.对数据进行ETL清洗 如果data为空，或者长度不满足，将数据过滤
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                jsonObj -> {
                    boolean flag = jsonObj.getString("table") != null
                            && jsonObj.getJSONObject("data") != null
                            && jsonObj.getString("data").length() >= 3;

                    return flag;
                }
        );

//        filterDS.print("json>>>");

        //动态分流 事实表放主流输出kafka dwd层 维度表 通过侧输出流 写入Hbase
        //5.1定义输出到Hbase测输出流标签
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE){};

        //5.2 主流 写回到Kafka
        SingleOutputStreamOperator<JSONObject> kafkaDS = filterDS.process(
                new TableProcessFunction(hbaseTag)
        );

        //5.3 获取侧输出流 写到Hbase
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);

        kafkaDS.print("kafka >>>>>");
        hbaseDS.print("hbase >>>>");




        env.execute();
    }

}
