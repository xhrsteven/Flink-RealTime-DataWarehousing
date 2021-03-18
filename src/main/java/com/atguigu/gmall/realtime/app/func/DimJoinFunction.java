package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * 维度关联接口
 * @param <T>
 */
public interface DimJoinFunction <T>{

    String getKey(T obj);

    void join(T obj, JSONObject dimInfoJsonObj) throws Exception;

}
