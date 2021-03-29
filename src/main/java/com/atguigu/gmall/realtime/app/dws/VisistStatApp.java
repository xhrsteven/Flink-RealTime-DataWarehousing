package com.atguigu.gmall.realtime.app.dws;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.VisitorStats;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;


import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class VisistStatApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/flink/checkpoint/VisitState"));
//        System.setProperty("HADOOP_USER_NAME","HIVE");


        //从Kafka的pv uv 跳转明细主题中获取数据
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueViewSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        String groupId = "visitor_stats_app";

        //从Kafka中读取数据
        FlinkKafkaConsumer<String> pageViewKafkasource = MyKafkaUtil.getKafkasource(pageViewSourceTopic, groupId);
        DataStreamSource<String> pageViewDS = env.addSource(pageViewKafkasource);

        FlinkKafkaConsumer<String> uniqueViewKafkasource = MyKafkaUtil.getKafkasource(uniqueViewSourceTopic, groupId);
        DataStreamSource<String> uniqueViewDS = env.addSource(uniqueViewKafkasource);

        FlinkKafkaConsumer<String> userJumpKafkasource = MyKafkaUtil.getKafkasource(userJumpDetailSourceTopic, groupId);
        DataStreamSource<String> userJumpDS = env.addSource(userJumpKafkasource);

//        pageViewDS.print("pageView>>>>>>>>>>");
//        uniqueViewDS.print("uniqueView>>>>>>>");
//        userJumpDS.print("userJump>>>>>>>>>>");

        //对各个流进行数据结构转换 jsonStr-> VisitorStats

        SingleOutputStreamOperator<VisitorStats> pageViewStatsDstream = pageViewDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                VisitorStats visitorStats = new VisitorStats(
                        "", "",
                        jsonObj.getJSONObject("common").getString("vc"),
                        jsonObj.getJSONObject("common").getString("ch"),
                        jsonObj.getJSONObject("common").getString("ar"),
                        jsonObj.getJSONObject("common").getString("is_new"),
                        0L, 1l, 0l, 0l,
                        jsonObj.getJSONObject("page").getLong("during_time"),
                        jsonObj.getLong("ts")
                );

                return visitorStats;
            }
        });

        SingleOutputStreamOperator<VisitorStats> uniqueVisitStatsDstream = uniqueViewDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                VisitorStats visitorStats = new VisitorStats(
                        "", "",
                        jsonObj.getJSONObject("common").getString("vc"),
                        jsonObj.getJSONObject("common").getString("ch"),
                        jsonObj.getJSONObject("common").getString("ar"),
                        jsonObj.getJSONObject("common").getString("is_new"),
                        0L, 1l, 0l, 0l,
                        jsonObj.getJSONObject("page").getLong("during_time"),
                        jsonObj.getLong("ts")
                );

                return visitorStats;
            }
        });


        SingleOutputStreamOperator<VisitorStats> userJumpStatDstream = userJumpDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                VisitorStats visitorStats = new VisitorStats(
                        "", "",
                        jsonObj.getJSONObject("common").getString("vc"),
                        jsonObj.getJSONObject("common").getString("ch"),
                        jsonObj.getJSONObject("common").getString("ar"),
                        jsonObj.getJSONObject("common").getString("is_new"),
                        0L, 1l, 0l, 0l,
                        jsonObj.getJSONObject("page").getLong("during_time"),
                        jsonObj.getLong("ts")
                );

                return visitorStats;
            }
        });

        SingleOutputStreamOperator<VisitorStats> sessionVisitDstream = pageViewDS.process(new ProcessFunction<String, VisitorStats>() {
            @Override
            public void processElement(String json, Context ctx, Collector<VisitorStats> out) throws Exception {
                JSONObject jsonObj = JSON.parseObject(json);
                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                if (lastPageId == null || lastPageId.length() == 0) {
                    VisitorStats visitorStats = new VisitorStats("", "",
                            jsonObj.getJSONObject("common").getString("vc"),
                            jsonObj.getJSONObject("common").getString("ch"),
                            jsonObj.getJSONObject("common").getString("ar"),
                            jsonObj.getJSONObject("common").getString("is_new"),
                            0L, 0L, 1L, 0L, 0L, jsonObj.getLong("ts")
                    );
                    out.collect(visitorStats);
                }
            }
        });

        //四条流结构相同合并
        DataStream<VisitorStats> unionDetailDS = uniqueVisitStatsDstream.union(
                pageViewStatsDstream,
                sessionVisitDstream,
                userJumpStatDstream
        );

        //开窗统计
        SingleOutputStreamOperator<VisitorStats> visitorStatsDS = unionDetailDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<VisitorStats>() {
                                    @Override
                                    public long extractTimestamp(VisitorStats visitorStats, long l) {
                                        return visitorStats.getTs();
                                    }
                                }
                        )
        );

        //分组统计 按照维度分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> visitorStatsTuple4KeyedStream = visitorStatsDS.keyBy(
                new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {

                    @Override
                    public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                        return Tuple4.of(
                                visitorStats.getAr(),
                                visitorStats.getCh(),
                                visitorStats.getVc(),
                                visitorStats.getIs_new()
                        );
                    }
                }
        );

                //开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS = visitorStatsTuple4KeyedStream.window(
                TumblingEventTimeWindows.of(Time.seconds(10))
        );

        //对窗口函数聚合

        SingleOutputStreamOperator<Object> reduceDS = windowDS.reduce(
                new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats stats1, VisitorStats stats2) throws Exception {
                        stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
                        stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());
                        stats1.setSv_ct(stats1.getSv_ct() + stats2.getSv_ct());
                        stats1.setDur_sum(stats1.getDur_sum() + stats1.getDur_sum());

                        return stats1;
                    }
                },
                new ProcessWindowFunction<VisitorStats, Object, Tuple4<String, String, String, String>, TimeWindow>() {

                    @Override
                    public void process(Tuple4<String, String, String, String> tuple4, Context context, Iterable<VisitorStats> elements, Collector<Object> out) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        for (VisitorStats visitorStats : elements) {
                            //窗口开始时间
                            String startDate = sdf.format(new Date(context.window().getStart()));
                            //窗口结束时间
                            String endDate = sdf.format(new Date(context.window().getEnd()));

                            visitorStats.setStt(startDate);
                            visitorStats.setEdt(endDate);
                            out.collect(visitorStats);
                        }
                    }
                }
        );


        env.execute();


    }
}
