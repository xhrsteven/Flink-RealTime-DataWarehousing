package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentInfo;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * 支付宽表处理主程序
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

//        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/flink/checkpoint/payment"));

        //从kafka topic中读取数据
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        String groupId = "paymentWide";

        FlinkKafkaConsumer<String> kafkaPaymentInfo = MyKafkaUtil.getKafkasource(paymentInfoSourceTopic, groupId);
        DataStreamSource<String> PaymentInfoDS = env.addSource(kafkaPaymentInfo);

        FlinkKafkaConsumer<String> OrderWideSource = MyKafkaUtil.getKafkasource(orderWideSourceTopic, groupId);
        DataStreamSource<String> OrderWideDS = env.addSource(OrderWideSource);

//        OrderWideDS.print(">>>");
        //对读取到的数据进行结构转换 json->POJO
        //转换支付流
//        SingleOutputStreamOperator<List<JSONObject>> paymentInfoDS1 = PaymentInfoDS.map(
//                new RichMapFunction<String, List<JSONObject>>() {
//                    @Override
//                    public List<JSONObject> map(String jsonStr) throws Exception {
//                        JSONArray jsonArr = JSON.parseArray(jsonStr);
//                        List<JSONObject> paymentInfo = new ArrayList<JSONObject>();
//                        for (int i = 0; i < jsonArr.size(); i++) {
//                            JSONObject jsonObj = jsonArr.getJSONObject(i);
//                            paymentInfo.add(jsonObj);
//                        }
//                        return paymentInfo;
//                    }
//                }
//        );
//
//        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentInfoDS1.flatMap(
//                new RichFlatMapFunction<List<JSONObject>, PaymentInfo>() {
//                    @Override
//                    public void flatMap(List<JSONObject> jsonObj, Collector<PaymentInfo> out) throws Exception {
//                        Iterator<JSONObject> it = jsonObj.iterator();
//                        while (it.hasNext()) {
//                            PaymentInfo paymentInfo = JSON.parseObject(it.next().toString(), PaymentInfo.class);
//                            out.collect(paymentInfo);
//                        }
//                    }
//                }
//        );

        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = PaymentInfoDS.map(
                jsonStr -> JSON.parseObject(jsonStr,PaymentInfo.class)
        );

//        paymentInfoDS.print("paymentInfo>>>>>>>>>>>");

        //转换订单宽表流
//        SingleOutputStreamOperator<OrderWide> orderWideDS1 = OrderWideDS.map(
//                new RichMapFunction<String, OrderWide>() {
//
//                    @Override
//                    public OrderWide map(String jsonStr) throws Exception {
//                        OrderWide orderWide = JSON.parseObject(jsonStr,OrderWide.class);
//                                return orderWide;
//                    }
//                }
//        );
        SingleOutputStreamOperator<OrderWide> orderWideDS = OrderWideDS.map(
                jsonStr ->JSON.parseObject(jsonStr,OrderWide.class)
        );


//        orderWideDS.print("order>>>>>>>");


        //设置Watermark 以及提取事件时间字段
        //支付流watermark
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWatermarkDS = paymentInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<PaymentInfo>() {
                                    @Override
                                    public long extractTimestamp(PaymentInfo paymentInfo, long recordTimestamp) {
                                        //需要将字符串时间转换为毫秒数
                                        return DateTimeUtil.toTs(paymentInfo.getCallback_time());
                                    }
                                }
                        )
        );


        //订单流watermark
        SingleOutputStreamOperator<OrderWide> orderWideWithWatermarkDS = orderWideDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderWide>() {
                                    @Override
                                    public long extractTimestamp(OrderWide orderWide, long recordTimpstamp) {
                                        return DateTimeUtil.toTs(orderWide.getCreate_time());
                                    }
                                }
                        )
        );


        //对数据进行分组
        //支付流分组
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedDS = paymentInfoWithWatermarkDS.keyBy(PaymentInfo::getId);

        KeyedStream<OrderWide, Long> orderWideKeyedDS = orderWideWithWatermarkDS.keyBy(OrderWide::getOrder_id);

        //使用IntervalJoin关联两条流

        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoKeyedDS.intervalJoin(orderWideKeyedDS)
                .between(Time.seconds(-1800), Time.seconds(0))
                .process(
                        new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {

                            @Override
                            public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context context, Collector<PaymentWide> out) throws Exception {
                                out.collect(new PaymentWide(paymentInfo, orderWide));
                            }
                        }
                );
//
        paymentWideDS.print("wide>>>>");

        paymentWideDS.map(
                paymentWide -> JSON.toJSONString(paymentWide)
        ).addSink(
                MyKafkaUtil.getKafkaSink(paymentWideSinkTopic)
        );


        env.execute();

    }

}
