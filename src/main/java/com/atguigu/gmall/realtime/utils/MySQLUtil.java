package com.atguigu.gmall.realtime.utils;

//查询数据的工具类

import com.atguigu.gmall.realtime.bean.TableProcess;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MySQLUtil {
    //ORM 查询，对象关系映射 OBJECT RELATION MAPPING
    public static <T> List<T> queryList(String sql, Class<T> clz, boolean underScoreToCamel) {
        //注册驱动
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            //创建连接
            conn = DriverManager.getConnection(
                    "jdbc:mysql://hadoop01:3306/gmall2021_realtime?characterEncoding=utf-8&useSSL=false",
                    "root",
                    "123456");
            //创建数据库操作对象
            ps = conn.prepareStatement(sql);

            //执行SQL语句
            rs = ps.executeQuery();

            //查询元数据信息
            ResultSetMetaData metaData = rs.getMetaData();

            List<T> resultList = new ArrayList<T>();
            //判断结果集是否存在数据，如果有，进行一次循环
            while(rs.next()){
                //创建一个对象，用于封装查询出来一条结果集中的数据
                T obj = clz.newInstance();
                //对查询的所有列进行遍历，获取每一列的名称
                for (int i = 1; i <=metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    String propertyName = "";
                 if (underScoreToCamel){
                     //如果指定将下划线转换为驼峰命名法的参数值为true，需要将表中列转化为驼峰命名法
                      propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                 }
                 //用apache的common给obj属性赋值
                    BeanUtils.setProperty(obj,propertyName,rs.getObject(i));
                }
                //将当前结果中的一行数据封装的obj对象放到list集合中
                resultList.add(obj);
            }

            // 处理结果集
            return resultList;

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从mysql查询数据失败");
        } finally {
            //释放资源
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        List<TableProcess> list = queryList("select * from table_process", TableProcess.class, true);
        for (TableProcess tableProcess : list) {
            System.out.println(tableProcess);
        }
    }
}
