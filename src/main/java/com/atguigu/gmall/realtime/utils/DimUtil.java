package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

/**
 * 用于维度查询的工具类，底层调用phoenix util
 *
 */
public class DimUtil {

        public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String,String> ... cloNameandValue){
            //拼接查询条件
            String whereSql = "where ";
            for (int i = 0; i < cloNameandValue.length; i++) {
                Tuple2<String,String> tuple2 = cloNameandValue[i];
                String filedName = tuple2.f0;
                String filedValue = tuple2.f1;
                if (i > 0) {
                    whereSql += " and ";
                }
                whereSql += filedName +"='" +filedValue + "'";
            }

            String sql = "select * from " + tableName + whereSql ;
            System.out.println("查询维度的SQL："+sql);

            List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);
            JSONObject dimJsonObj =null;
            //对于维度查询来讲，一般都是根据主键进行查询，不可能返回多条记录，只会有一条
            if (dimList != null && dimList.size() >0) {
                 dimJsonObj = dimList.get(0);
            }else{
                System.out.println("维度数据没有找到:" + sql);
            }
            return dimJsonObj;
        }

//    public static void main(String[] args) {
//        JSONObject dimInfo = DimUtil.getDimInfoNoCache("DIM_BASE_TRADEMARK", Tuple2.of("id", "13"));
//
//        System.out.println(dimInfo);
//    }
}
