package com.atguigu.gmall.realtime.app.func;

//写出维度数据Sink实现类

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.DimUtil;
import net.minidev.json.writer.ArraysMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Locale;
import java.util.Set;

public class DimSink extends RichSinkFunction<JSONObject> {
    //定义链接
    private Connection conn = null;

    @Override
    public void open(Configuration parameters) throws Exception {
    //对链接对象初始化
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }
    //对流中数据进行处理
    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        //获取目标表的名称
        String tableName = jsonObj.getString("sink_table");
        //获取json中data
        JSONArray jsonArr = jsonObj.getJSONArray("data");
        for (int i = 0; i < jsonArr.size(); i++) {
            JSONObject dataJsonObj = jsonArr.getJSONObject(i);
            if (dataJsonObj != null && dataJsonObj.size()>0) {
                //根据data中属性和属性值 生产upsert语句
                String upsertSql = genUpsertSql(tableName.toUpperCase(),dataJsonObj);
                System.out.println("向Phoenix插入数据的SQL： " + upsertSql);

                PreparedStatement ps = null;
                //执行SQL
                try {
                    ps = conn.prepareStatement(upsertSql);
                    ps.execute();
                    //注意 执行完Phoenix插入操作，需要手动提交事务
                    conn.commit();
                    ps.close();
                }catch (SQLException e){
                    e.printStackTrace();
                    throw new RuntimeException("插入数据失败");
                }finally {
                    if(ps != null){
                        ps.close();
                    }
                }
                //如果当前做的是更新操作，需要将redis中的缓存数据清除
                if (jsonObj.getString("type").equals("UPDATE")) {
                    DimUtil.deleteCached(tableName,dataJsonObj.getString("id"));
                }
            }
        }

    }
    //根据data属性和值 生成向Phoenix中插入数据的SQL语句
    private String genUpsertSql(String tableName, JSONObject dataJsonObj) {

        Set<String> keys = dataJsonObj.keySet();
        Collection<Object> values = dataJsonObj.values();

        String upsertSql = "upsert into "+ GmallConfig.HBASE_SCHEMA + "."+ tableName +"("+
                StringUtils.join(keys,",") + ")";

        String valueSql = "values ('"+ StringUtils.join(values,"','") + "')";

        return upsertSql + valueSql;
    }
}
