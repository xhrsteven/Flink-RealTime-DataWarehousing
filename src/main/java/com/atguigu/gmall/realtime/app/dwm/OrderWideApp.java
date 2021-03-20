package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.app.func.DimSink;
import com.atguigu.gmall.realtime.bean.OrderDetail;
import com.atguigu.gmall.realtime.bean.OrderInfo;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.sun.org.apache.xpath.internal.operations.Or;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 处理订单宽表合并
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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


//        //2.2读取订单明细数据
        FlinkKafkaConsumer<String> orderDetailSource = MyKafkaUtil.getKafkasource(orderDetailSourceTopic, groupId);

        DataStreamSource<String> orderDetailJsonStrDS = env.addSource(orderDetailSource);


//        //3.string -> json -> 实体类对象
//        //3.1 转换订单数据结构
        SingleOutputStreamOperator<List<JSONObject>> OrderInfoDS1 = orderInfoJsonStrDS.map(
                new RichMapFunction<String, List<JSONObject>>() {

                    @Override
                    public List<JSONObject> map(String jsonStr) throws Exception {
                        JSONArray jsonArr = JSON.parseArray(jsonStr);
//                        System.out.println(jsonStr);
                        List<JSONObject> orderInfo= new ArrayList<JSONObject>();
                        for (int i = 0; i < jsonArr.size(); i++) {
                            JSONObject jsonObj = jsonArr.getJSONObject(i);
                            orderInfo.add(jsonObj);
                        }
                        return  orderInfo;
                    }
                });

        SingleOutputStreamOperator<OrderInfo> OrderInfoDS = OrderInfoDS1.flatMap(
                new RichFlatMapFunction<List<JSONObject>, OrderInfo>() {
                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }
                    @Override
                    public void flatMap(List<JSONObject> jsonObjects, Collector<OrderInfo> collector) throws Exception {
                        Iterator<JSONObject> iter = jsonObjects.iterator();
                        while (iter.hasNext()) {
                            OrderInfo orderInfo = JSON.parseObject(iter.next().toString(), OrderInfo.class);
                            orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                            orderInfo.setCreate_date(orderInfo.getCreate_date());
                            collector.collect(orderInfo);
                        }
                    }
                });


//        //3.2转换订单明细数据结构
        SingleOutputStreamOperator<List<JSONObject>> OrderDetailDS1 = orderDetailJsonStrDS.map(
                new MapFunction<String, List<JSONObject>>() {
                    @Override
                    public List<JSONObject> map(String jsonStr) throws Exception {
                        JSONArray jsonArr = JSON.parseArray(jsonStr);
//                        System.out.println(jsonStr);
                        List<JSONObject> orderDetail= new ArrayList<JSONObject>();
                        for (int i = 0; i < jsonArr.size(); i++) {
                            JSONObject jsonObj = jsonArr.getJSONObject(i);
                            orderDetail.add(jsonObj);
                        }
                              return  orderDetail;
                    }
                }
        );

        SingleOutputStreamOperator<OrderDetail> OrderDetailDS = OrderDetailDS1.flatMap(
                new RichFlatMapFunction<List<JSONObject>, OrderDetail>() {
                    SimpleDateFormat sdf = null;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }
                    @Override
                    public void flatMap(List<JSONObject> jsonObjects, Collector<OrderDetail> collector) throws Exception {
                        Iterator<JSONObject> iter = jsonObjects.iterator();
                        while (iter.hasNext()) {
                            OrderDetail orderDetail = JSON.parseObject(iter.next().toString(), OrderDetail.class);
                            orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                            collector.collect(orderDetail);

                        }
                    }
                });


//        OrderInfoDS.print("orderInfo>>>>>>");
//        OrderDetailDS.print("orderDetail>>>>>>>>>>");
//
//        //指定事件时间字段--假设有序时间
//        //4.1订单
//        SingleOutputStreamOperator<OrderInfo> orderInfoWithTsDS =
//                OrderInfoDS.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderInfo>() {
//                    @Override
//                    public long extractAscendingTimestamp(OrderInfo orderInfo) {
//                        return orderInfo.getCreate_ts();
//                    }
//                });

        SingleOutputStreamOperator<OrderInfo> orderInfoWithTsDS = OrderInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
                                return orderInfo.getCreate_ts();
                            }
                        })
        );
//
////        4.2订单明细
//
//        SingleOutputStreamOperator<OrderDetail> orderDetailWithTsDS = OrderDetailDS.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderDetail>() {
//            @Override
//            public long extractAscendingTimestamp(OrderDetail orderDetail) {
//                return orderDetail.getCreate_ts();
//            }
//        });

        SingleOutputStreamOperator<OrderDetail> orderDetailWithTsDS = OrderDetailDS.assignTimestampsAndWatermarks(

                WatermarkStrategy.
                        <OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(10)) //
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail orderDetail, long recordTimestamp) {
                                return orderDetail.getCreate_ts();
                            }
                        })
        );
//
//
//        //5.按照订单id 进行分组，指定关联key
        KeyedStream<OrderInfo, Long> orderInfoKeyedDS = orderInfoWithTsDS.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailKeyedDS = orderDetailWithTsDS.keyBy(OrderDetail::getOrder_id);
//
//        //6. 使用interval Join 进行关联
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoKeyedDS
                .intervalJoin(orderDetailKeyedDS)
                .between(Time.milliseconds(-3600), Time.milliseconds(10))
                .process(
                        new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                            @Override
                            public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context cxt, Collector<OrderWide> out) throws Exception {
                                out.collect(new OrderWide(orderInfo, orderDetail));
                            }
                        }
                );
//
//        orderWideDS.print("orderWide>>>>>>>");

//关联用户维度 -- 无序等待

        SingleOutputStreamOperator<OrderWide> orderWideWithUserDstream =
                AsyncDataStream.unorderedWait(orderWideDS, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {

            @Override
            public String getKey(OrderWide obj) {
                return String.valueOf(obj.getUser_id());
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                String birthday = dimInfoJsonObj.getString("BIRTHDAY");
                //定义日期转换工具
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

                Date birthdayDate = sdf.parse(birthday);
                Long curTs = System.currentTimeMillis();
                Long betweenMs = curTs - birthdayDate.getTime();
                Long ageLong = betweenMs / 1000L / 60L / 60L / 24L / 365L;
                Integer age = ageLong.intValue();
                orderWide.setUser_age(age);
                orderWide.setUser_gender(dimInfoJsonObj.getString("GENDER"));
            }
        }, 10000, TimeUnit.SECONDS, 1000);

//        orderWideWithUserDstream.print(">>>>>");

        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(
                orderWideWithUserDstream,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                        orderWide.setProvince_name(dimInfoJsonObj.getString("NAME"));
                        orderWide.setProvince_area_code(dimInfoJsonObj.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(dimInfoJsonObj.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(dimInfoJsonObj.getString("ISO_3166_2"));
                    }
                },1000,TimeUnit.SECONDS
        );

//     orderWideWithProvinceDS.print("province>>>>>");
// 关联sku维度

        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                        orderWide.setSku_name(dimInfoJsonObj.getString("SKU_NAME"));
                        orderWide.setCategory3_id(dimInfoJsonObj.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(dimInfoJsonObj.getLong("SPU_ID"));
                        orderWide.setTm_id(dimInfoJsonObj.getLong("TM_ID"));
                    }
                }, 1000, TimeUnit.SECONDS
        );

//      orderWideWithSkuDS.print("sku>>>>>");

//关联SPU商品维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSpu_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                        orderWide.setSku_name(dimInfoJsonObj.getString("SPU_NAME"));
                    }
                }, 1000, TimeUnit.SECONDS
        );

//        orderWideWithSpuDS.print("spu>>>>>>>>>>");

        //关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategoryDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getCategory3_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                        orderWide.setCategory3_name(dimInfoJsonObj.getString("NAME"));
                    }
                }, 1000, TimeUnit.SECONDS
        );

//        orderWideWithCategoryDS.print("category>>>>>>>>>>>>");
        //关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithCategoryDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getTm_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                        orderWide.setTm_name(dimInfoJsonObj.getString("TM_NAME"));
                    }
                }, 1000, TimeUnit.SECONDS
        );

//        orderWideWithTmDS.print("tm>>>>>>>>>>");

        orderWideWithTmDS.map(orderWide -> JSON.toJSONString(orderWide))
                .addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));



        env.execute();
    }

}
