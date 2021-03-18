package com.atguigu.gmall.realtime.app.dwd;
//准备业务数据的DWD层数据

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimSink;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;


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
        String groupId = "ods_base_group2";

        //2.1通过工具类获取Kafka消费者
        FlinkKafkaConsumer<String> kafkasource = MyKafkaUtil.getKafkasource(topic, groupId);

        DataStreamSource<String> jsonStrDS = env.addSource(kafkasource);

//        jsonStrDS.print("kafkajson>>>>>>>>");

        //对DS中数据进行结构转换 String -> Json

        //对数据进行结构的转换 String->JSONObject
        DataStream<JSONObject> jsonObjDS = jsonStrDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                String s1 = s.replace("null", "''");
                return  JSON.parseObject(s1);

            }
        });

//       jsonObjDS.print("json>>>>>");
//        jsonDS.map(JSON::parseObject);

        //3.对数据进行ETL清洗 如果data为空，或者长度不满足，将数据过滤
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                jsonObj -> {
                    boolean flag = jsonObj.getString("table") != null
                            && jsonObj.getJSONArray("data") != null
                            && jsonObj.getString("data").length() >= 3;
                    return flag;
                }
        );

//      filterDS.print("filterJson>>>");

        //动态分流 事实表放主流输出kafka dwd层 维度表 通过侧输出流 写入Hbase
//        5.1定义输出到Hbase测输出流标签
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE){};

        //5.2 主流 写回到Kafka 使用自定义 ProcessFunction 进行分流处理
        SingleOutputStreamOperator<JSONObject> kafkaDS = filterDS.process(
                new TableProcessFunction(hbaseTag)
        );

        //5.3 获取侧输出流 写到Hbase 获取侧输出流，即将通过 Phoenix 写到 Hbase 的数据
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);

//         kafkaDS.print("kafka >>>>>");
//        hbaseDS.print("hbase >>>>");

        //6.将维度数据保存到Phoenix对应的维度表中
        hbaseDS.print("hbase :::::");
        hbaseDS.addSink(new DimSink());
//
        //7.将事实数据写回到kafka的dwd层
        FlinkKafkaProducer<JSONObject> kafkaSink = MyKafkaUtil.getKafkaSinkBySchema(
                new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public void open(SerializationSchema.InitializationContext context) throws Exception {
                        System.out.println("启动 Kafka Sink");
                    }
                    //从每条数据得到该条数据应送往的主题名
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long aLong) {
                        String sinkTopic = jsonObj.getString("sink_table");

                        JSONArray jsonArr = jsonObj.getJSONArray("data");

                        return new ProducerRecord(sinkTopic, jsonArr.toString().getBytes());

                    }
                }
        );
        kafkaDS.print("kafka ::::");
        kafkaDS.addSink(kafkaSink);

        env.execute();
    }

}
