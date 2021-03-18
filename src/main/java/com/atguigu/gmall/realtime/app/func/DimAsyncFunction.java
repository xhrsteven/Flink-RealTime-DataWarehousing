package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.DimUtil;
import com.atguigu.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/*
维度自定义异步查询
模板方法设计模式：
在父类中只定义
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T>{
    //多态
    private ExecutorService executorService ;

    private String tableName;

    public DimAsyncFunction(String tableName){
        this.tableName = tableName;
    }

    //需要提供一个获取key的方法
    public abstract String getKey(T obj);

    public abstract void join(T obj, JSONObject dimInfoJsonObj);


    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化线程池对象
        System.out.println("初始化线程池对象");
        executorService = ThreadPoolUtil.getInstance();


    }

    /*
    发送异步请求
    @input 流中事实数据
    @result 异步处理结束之后 返回结果
     */
    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        executorService.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        //异步请求
                        try {
                            long start = System.currentTimeMillis();
                            //从流中事实数据获取
                            String key = getKey(obj);
                            //根据维度id 到维度表中进行查询
                            JSONObject dimInfoJsonObj = DimUtil.getDimInfo(tableName, key);
                            System.out.println("维度数据Json格式：" + dimInfoJsonObj);

                            //维度关联 流中事实数据和查询出来的维度数据进行关联
                            if (dimInfoJsonObj != null) {
                                join(obj, dimInfoJsonObj);
                            }
                            System.out.println("维度关联后的对象：" + obj);
                            long end = System.currentTimeMillis();
                            System.out.println("异步维度查询耗时：" + (end - start) + "毫秒");
                            //将关联后的数据向下传递
                            resultFuture.complete(Arrays.asList(obj));
                        }catch (Exception e){
                            e.printStackTrace();
                            throw new RuntimeException("维度异步查询失败");
                        }
                    }
                }
        );
    }
}
