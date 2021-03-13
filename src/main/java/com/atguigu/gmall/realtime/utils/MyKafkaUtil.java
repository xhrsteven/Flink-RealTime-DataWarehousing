package com.atguigu.gmall.realtime.utils;

//操作kafka工具类

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class MyKafkaUtil {
    private static String KafkaServer = "hadoop01:9092,hadoop02:9092,hadoop03:9092";
    private static String DEFAULT_TOPIC="DEFAULT_DATA";

    //获取flinkKafkaConsumer
    public static FlinkKafkaConsumer<String> getKafkasource(String topic,String groupId){
        //Kafka链接属性
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KafkaServer);

        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),props);
    }

    //封装kafka生产者
    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        return new FlinkKafkaProducer<String>(KafkaServer, topic,new SimpleStringSchema());
    }

    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema){
        //kafka基本配置信息
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KafkaServer);

        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"");
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC,kafkaSerializationSchema,props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }


}
