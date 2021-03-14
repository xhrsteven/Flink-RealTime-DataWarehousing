package com.atguigu.gmall.realtime.app.dwm;

import akka.stream.actor.WatermarkRequestStrategy;
import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.bean.OrderDetail;
import com.atguigu.gmall.realtime.bean.OrderInfo;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
//import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * 处理订单宽表合并
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(5);

        //ck 设置检查点
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/flink/checkpoint/uniqueVisitor"));
        /**
         * 从kafka的dwd层读取订单和订单明细数据
         */
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String groupId = "order_wide_group";
        String orderWideSinkTopic = "dwm_order_wide";

        //2.1 读取订单数据
        FlinkKafkaConsumer<String> orderInfoSource = MyKafkaUtil.getKafkasource(orderInfoSourceTopic, groupId);

        DataStreamSource<String> orderInfoJsonStrDS = env.addSource(orderInfoSource);

        //2.2读取订单明细数据
        FlinkKafkaConsumer<String> orderDetailSource = MyKafkaUtil.getKafkasource(orderDetailSourceTopic, groupId);

        DataStreamSource<String> orderDetailJsonStrDS = env.addSource(orderDetailSource);

        //3.string -> json -> 实体类对象
        //3.1 转换订单数据结构
        SingleOutputStreamOperator<OrderInfo> OrderInfoDS = orderInfoJsonStrDS.map(
                new RichMapFunction<String, OrderInfo>() {
                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderInfo map(String jsonStr) throws Exception {
                        OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
                        orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_date()).getTime());
                        return orderInfo;
                    }
                }
        );

        //3.2转换订单明细数据结构
        SingleOutputStreamOperator<OrderDetail> OrderDetailDS = orderDetailJsonStrDS.map(
                new RichMapFunction<String, OrderDetail>() {
                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderDetail map(String jsonStr) throws Exception {

                        OrderDetail orderDetail = JSON.parseObject(jsonStr, OrderDetail.class);
                        orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                        return orderDetail;
                    }
                }
        );

//        OrderInfoDS.print("orderInfo>>>>>>>>>");
//        OrderDetailDS.print("orderDetail>>>>>>>>>>");

        //指定事件时间字段
        //4.1订单
//        SingleOutputStreamOperator<OrderInfo> orderInfoWithTsDS = OrderInfoDS.assignTimestampsAndWatermarks(
//                WatermarkStrategy
//                        .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
//                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
//                            @Override
//                            public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
//                                return orderInfo.getCreate_ts();
//                            }
//                        })
//        );
        
        //4.2订单明细
//        SingleOutputStreamOperator<OrderDetail> orderDetailWithTsDS = OrderDetailDS.assignTimestampsAndWatermarks(
//                WatermarkStrategy.
//                        <OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
//                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
//                            @Override
//                            public long extractTimestamp(OrderDetail orderDetail, long recordTimestamp) {
//                                return orderDetail.getCreate_ts();
//                            }
//                        })
//        );


        //5.按照订单id 进行分组，指定关联key
        KeyedStream<OrderInfo, Long> orderInfoKeyedDS = OrderInfoDS.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailKeyedDS = OrderDetailDS.keyBy(OrderDetail::getOrder_id);
        
        //6. 使用interval Join 进行关联
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoKeyedDS
                .intervalJoin(orderDetailKeyedDS)
                .between(Time.milliseconds(-5), Time.milliseconds(5))
                .process(
                        new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                            @Override
                            public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context cxt, Collector<OrderWide> out) throws Exception {
                                out.collect(new OrderWide(orderInfo, orderDetail));
                            }
                        }
                );

        orderWideDS.print("orderWide>>>>>>>");


        env.execute();
    }

}
