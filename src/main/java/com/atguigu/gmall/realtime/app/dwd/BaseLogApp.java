package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

//准备用户行为日志DWD层
public class BaseLogApp {
//    @lombok.SneakyThrows

    private  static final String TOPIC_START="dwd_start_log";
    private  static final String TOPIC_DISPLAY="dwd_display_log";
    private  static final String TOPIC_PAGE="dwd_page_log";

    public static void main(String[] args) throws Exception{
        //1.1 准备环境 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.2设置并行度--Kafka分区
        env.setParallelism(5);

        //1.3设置checkpoint,每5000ms开始一次checkpoint，模式默认EXACTLY_ONCE

//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/flink/checkpoint/baselogApp"));

//        System.setProperty("HADOOP_USER_NAME","root");

        //2.从kafka读取数据
        //2.1调用Kafka工具类 获取FlinkKafkaConsumer
        String topic = "ods_base_log";
        String groupId = "base_log_app_group";

        FlinkKafkaConsumer<String> kafkasource = MyKafkaUtil.getKafkasource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkasource);

        //3.对读取到的数据格式进行转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        return jsonObject;
                    }
                }
        );

//      jsonObjDS.print("json>>>>>>>>>>>>>>>>>>");

        //4.识别新老访客
        //保存mid某天方法情况（将首次访问日期作为状态保存起来） 在有日志过来的时候，从状态中获取日期
        //和日志产生日志进行对比，如果状态不为空，
        //4.1 根据mid 对日志进行分组

        KeyedStream<JSONObject, String> midKeyedDS = jsonObjDS.keyBy(
                data -> data.getJSONObject("common").getString("mid")
        );

        //4.2新老方法状态修复 状态分为算子状态和监控状态，我们这里要记录一个设备的访问，使用key状态

        SingleOutputStreamOperator<JSONObject> jsonDSWithFlag = midKeyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    //定义mid 访问状态
                    private ValueState<String> firstVisitDateState;
                    //定义日期格式化对象
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                       //对状态以及日期格式进行初始化
                        firstVisitDateState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("newMidDateState",String.class)
                        );
                        sdf = new SimpleDateFormat("yyyyMMdd");
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
                        //日志标记状态
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");

                        //获取日志访问时间戳

                        Long ts = jsonObj.getLong("ts");

                        if("1".equals(isNew)){

                            String stateDate = firstVisitDateState.value();

                            //对日志日期格式进行转换
                            String tsDate = sdf.format(new Date(ts));

                            if(stateDate != null && stateDate.length()!=0){
                                //判断是否为同一天数据
                                if(stateDate.equals(tsDate)){
                                    isNew="0";
                                    jsonObj.getJSONObject("common").put("is_new",isNew);
                                }
                            }else{
                                //将当前访问日期作为状态值
                                firstVisitDateState.update(tsDate);
                            }
                        }
                        return jsonObj;
                    }
                }
        );
//        jsonDSWithFlag.print(">>>>>>>>>>>>");

        //5.分流操作 （页面，启动，曝光）
        //侧输出流： 接收迟到数据， 分流

        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>("display"){};

        SingleOutputStreamOperator<String> pageDS=jsonDSWithFlag.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<String> out) throws Exception {
                        //获取启动日志标记
                        //将JSON格式转换成字符串
                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        String dataStr = jsonObj.toString();

                        //判断是否为启动日志
                        if(startJsonObj!=null && startJsonObj.size()>0){
                            //如果是启动日志，输出到启动侧输出流
                            ctx.output(startTag,dataStr);
//                            System.out.println(">>>"+startJsonObj);
                        }else{
                            out.collect(dataStr);
                            //如果不是启动日志 则为页面日志或者曝光日志(携带页面信息)
//                            System.out.println("PageString:" + dataStr);

                            JSONArray displays = jsonObj.getJSONArray("displays");
//                            System.out.println("dis>>>>>"+displays);

                            if(displays !=null && displays.size()>0){
                                //遍历曝光日志 输出到侧输出流
                                for(int i = 0; i< displays.size(); i++){
                                    JSONObject displayJsonObj = displays.getJSONObject(i);
                                    //获取页面id
                                    String pageId = jsonObj.getJSONObject("page").getString("page_id");
                                    //给每条曝光信息添加上 pageId
                                    displayJsonObj.put("page_id",pageId);
                                    //将曝光数据输出到测输出流
                                    ctx.output(displayTag,displayJsonObj.toString());
                                }
                            }

                        }

                    }
                }
        );

        //获取侧输出流
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        pageDS.print("page>>>>");
        startDS.print("start>>>>");
        displayDS.print("display>>>>");

        //6.将不同的流写回到不同的topic中 --dwd层
        FlinkKafkaProducer<String> StartSink = MyKafkaUtil.getKafkaSink(TOPIC_START);

        startDS.addSink(StartSink);

        FlinkKafkaProducer<String> DisplaySink = MyKafkaUtil.getKafkaSink(TOPIC_DISPLAY);

        displayDS.addSink(DisplaySink);

        FlinkKafkaProducer<String> PageSink = MyKafkaUtil.getKafkaSink(TOPIC_PAGE);

        pageDS.addSink(PageSink);



        env.execute();

    }
}
