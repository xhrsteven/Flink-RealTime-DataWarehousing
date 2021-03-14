package com.atguigu.gmall.realtime.app.dwm;

/**
 *独立访客UV的计算
 * 模拟生产日志jar -> nginx -> 日志采集服务 -> kafka(ods)
 * -> BaseLogApp(分流) -> Kafka(dwd) dwd_page_log
 * -> UniqueVisitApp(独立访客) -> Kafka(dwm_unique_visit)
 */
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;

public class UniqueVisitApp {
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
        String groupId = "Unique_Visit_Group";
        String sinkTopic = "dwm_unique_visit";

        FlinkKafkaConsumer<String> kafkasource = MyKafkaUtil.getKafkasource(sourceTopic, groupId);

        DataStreamSource<String> jsonStrDS = env.addSource(kafkasource);

        //3.对读取到的数据进行结构转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr));

//      jsonObjDS.print("test>>>");

        //按照设备id进行分组
        KeyedStream<JSONObject, String> keyByMidDS = jsonObjDS.keyBy(
                jsonObj -> jsonObj.getJSONObject("common").getString("mid")
        );

        SingleOutputStreamOperator<JSONObject> filteredDS = keyByMidDS.filter(
                new RichFilterFunction<JSONObject>() {
                    //定义状态
                    ValueState<String> lastVisitDateState = null;
                    //定义日期工具类
                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化日期工具类
                        sdf = new SimpleDateFormat("yyyyMMdd");
                        //初始化状态
                        ValueStateDescriptor<String> lastVisitDateStateDes =
                                new ValueStateDescriptor<>("lastVisitDateState", String.class);
                        //因为我们统计的是日活DAU，所以状态数据只在当天有效 ，过了一天就可以失效掉
                        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();
                        lastVisitDateStateDes.enableTimeToLive(stateTtlConfig);
                        this.lastVisitDateState = getRuntimeContext().getState(lastVisitDateStateDes);
                    }

                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        //首先判断当前页面是否从别的页面跳转过来的
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPageId != null && lastPageId.length() > 0) {
                            return false;
                        }

                        //获取当前访问时间
                        Long ts = jsonObj.getLong("ts");
                        //将当前访问时间戳转换为日期字符串
                        String logDate = sdf.format(new Date(ts));
                        //获取状态日期
                        String lastVisitDate = lastVisitDateState.value();

                        //用当前页面的访问时间和状态时间进行对比
                        if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(logDate)) {
                            System.out.println("已访问：lastVisitDate-" + lastVisitDate + ",||logDate:" + logDate);
                            return false;
                        } else {
                            System.out.println("未访问：lastVisitDate-" + lastVisitDate + ",||logDate:" + logDate);
                            lastVisitDateState.update(logDate);
                            return true;
                        }
                    }
                }
        );
//        filteredDS.print("filter>>>>");
        //想kafka中写回 需要将json转换回string
        // json -> String
        SingleOutputStreamOperator<String> kafkaDS = filteredDS.map(jsonObj -> jsonObj.toJSONString());

        //写回kafka dwm 层
        kafkaDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        env.execute();
    }

}
