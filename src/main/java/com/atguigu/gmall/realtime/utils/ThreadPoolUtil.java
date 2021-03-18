package com.atguigu.gmall.realtime.utils;

import org.apache.flink.util.TimeUtils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/*
创建单例线程池对象
 */
  /*
        corePoolSize:线程数量
        maximumPoolSize:最大容量
        KeepAliveTime:时间
        unit:
        workQueue:
         */
public class ThreadPoolUtil {
    private  static ThreadPoolExecutor pool ;
    public static ThreadPoolExecutor getInstance(){
        if (pool == null) {
            synchronized (ThreadPoolUtil.class){
                if (pool == null) { //双重校验
                    pool = new ThreadPoolExecutor(
                            4,20,300, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE)
                    );
                }
            }
        }

        return pool;
    }
}
