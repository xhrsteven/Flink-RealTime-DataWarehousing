package com.atguigu.gmall.realtime.app.dwm;
/*
CEP
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
//import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 用户跳出行为过滤
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(6);
        //ck 设置检查点
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/flink/checkpoint/uniqueVistor"));

        //2.从Kafka中读取数据
        String sourceTopic = "dwd_page_log";
        String groupId = "User_Jumper_Group";
        String sinkTopic = "dwm_user_jump_detatil";

        FlinkKafkaConsumer<String> kafkasource = MyKafkaUtil.getKafkasource(sourceTopic, groupId);

        DataStreamSource<String> jsonStrDS = env.addSource(kafkasource);

        //3.对读取到的数据进行结构转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr));

//        jsonObjDS.print("JSON>>>>");
        //注意 Flink1.12开始 默认时间语义就是事件时间，不需要额外指定，之前版本，需要如下语句指定事件时间语义
//                env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //4.指定事件时间字段 --没有乱序的情况
//        SingleOutputStreamOperator<JSONObject> jsonObjWithTSDS = jsonObjDS.assignTimestampsAndWatermarks(
//                WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(
//                        new SerializableTimestampAssigner<JSONObject>() {
//                            @Override
//                            public long extractTimestamp(JSONObject jsonObj, long l) {
//                                return jsonObj.getLong("ts");
//                            }
//                        }
//                ));

        //5.按照mid进行分组
        KeyedStream<JSONObject, String> keyByMidDS = jsonObjDS.keyBy(
                jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //确认CEP依赖
        //配置CEP表达式
        /**
         * 计算页面跳出明细，需要满足两个条件
         * 1.不是从其他页面跳转过来的页面，是一个首次访问页面
         *  last_page_id == null
         *
         *  2.距离首次访问结束后10内，没有对其他页面进行访问
         */
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first")
                .where(
                        //条件1
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject jsonObj) throws Exception {
                                //获取last_page_id
                                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                                //判断是否为null 非空的过滤
                                if (lastPageId == null || lastPageId.length() == 0) {
                                    return true;
                                }
                                return false;
                            }
                        }
                )
                .next("next") //严格连续
                .where(
                        //条件2
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject jsonObj) throws Exception {
                                //获取当前页面ID
                                String PageId = jsonObj.getJSONObject("page").getString("page_id");
                                //判断访问页面是否为空
                                if (PageId != null && PageId.length() > 0) {
                                    return true;
                                }
                                return false;
                            }
                        }
                ).within(Time.milliseconds(10000));

        // 7.根据CEP表达式筛选流
        PatternStream<JSONObject> patternStream = CEP.pattern(keyByMidDS, pattern);

        // 8.从筛选之后的流中 提取数据，将超时数据放到侧输出流
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){};

//        patternStream.select(
//                new PatternSelectFunction<JSONObject, String>() {
//                    @Override
//                    public String select(Map<String, List<JSONObject>> map) throws Exception {
//                        return null;
//                    }
//                }
//        )

        SingleOutputStreamOperator<String> filterTimeoutDS = patternStream.flatSelect(
                timeoutTag,
                //处理超时数据
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestmap, Collector<String> out) throws Exception {
                        //获取所有符合first的对象
                        List<JSONObject> jsonObjList = pattern.get("first");
                        //在timeout方法中的数据都会被参数1中的标签标记
                        for (JSONObject jsonObject : jsonObjList) {
                            out.collect(jsonObject.toJSONString());
                        }
                    }
                },
                //处理没有超时数据
                new PatternFlatSelectFunction<JSONObject, String>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> map, Collector<String> collector) throws Exception {
                        //没有超时数据不在统计范围之内

                    }
                }
        );
        //从侧输出流中获取数据
        DataStream<String> jumpDS = filterTimeoutDS.getSideOutput(timeoutTag);

        jumpDS.print("time>>>>");

        //将跳出数据写回到kafka dwm
        jumpDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));


        env.execute();
    }
}
