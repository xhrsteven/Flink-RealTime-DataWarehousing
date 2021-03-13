package com.atguigu.gmall.realtime.app.func;

//配置表函数

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.MySQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction extends ProcessFunction<JSONObject, JSONObject> {
    //维度数据通过侧输出流输出 ，定义侧输出流标记
    private OutputTag<JSONObject> outputTag;

    //用于内存中存放配置表的Map《表名：操作，tableProcess》
    private Map<String,TableProcess> tableProcessMap = new HashMap<>();

    //用于在内存中存放已经在phoenix中已经建过的表
    private Set<String> existsTables = new HashSet<>();

    //声明Phoenix连接变量
    Connection conn = null;
    //实例化函数对象时，将侧输出流标签也进行赋值
    public TableProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    //函数被调用的时候执行的方法 执行一次(生命周期方法)
    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化Phoenix连接
        Class.forName("com.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        //初始化配置表信息
        refreshMeta();
        //配置表数据发送变化，需要开启一个定时任务 需要每隔一段时间从配置表中查询一次数据，更新到map 并检查建表
        //从现在起，delay 5s后，每隔period 5s执行一次
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                refreshMeta();
            }
        },5000,5000);
    }

    public void refreshMeta(){
        // =================从mysql数据库配置表中查询配置信息==========================
        System.out.println("查询配置表信息");
        List<TableProcess> tableProcessList = MySQLUtil.queryList("select * from table_process", TableProcess.class, true);
        for (TableProcess tableProcess:tableProcessList) {
            //获取源表名
            String sourceTable = tableProcess.getSourceTable();
            //获取操作类型
            String operateType = tableProcess.getOperateType();
            //输出类型
            String sinkType = tableProcess.getSinkType();
            //输出目的地表名或topic名
            String sinkTable = tableProcess.getSinkTable();
            //输出字段
            String sinkColumns = tableProcess.getSinkColumns();
            //表的主键
            String sinkPk = tableProcess.getSinkPk();
            //扩展语句
            String sinkExtend = tableProcess.getSinkExtend();
            //拼接报错配置的key
            String key = sourceTable+ ":"+ operateType;
            //===================将从配置表中查询到配置信息，保存到内存的map集合中===============
            tableProcessMap.put(key,tableProcess);

            //===================如果当前配置项是维度表需要向HBASE表中保存数据 判断Phoenix中是否存在这张表=======
            if (TableProcess.SINK_TYPE_HBASE.equals(sinkType) && "insert".equals(operateType)){
                boolean notExist = existsTables.add(sourceTable);
                //如果内存set集合中不存在这个表 那么创建表
                if (notExist){
                    //检查Phoenix中是否存在这种表
                    // 有可能存在 只不过是应用缓存被清空了，导致当前表没有缓存 这种情况不续签创建表
                    checkTable(sinkTable,sinkColumns,sinkPk,sinkExtend);
                }
            }
        }
        if (tableProcessMap == null || tableProcessMap.size() == 0) {
            throw new RuntimeException("没有读到数据库配置表中数据");
        }
    }

    private void checkTable(String tableName, String fields,String pk, String ext) {
        //如果在配置表中没有配置主键，需要给主键默认值
        if (pk == null) {
            pk = "id";
        }
        //如果在配置表中没有配置扩展值，需要给默认扩展默认值
        if (ext == null) {
            ext = "";
        }
        //拼接字符串
        StringBuilder createSql = new StringBuilder("create table if not exits " +
                GmallConfig.HBASE_SCHEMA + "." + tableName + "(");
        //对建表字段切分
        String[] fieldArr = fields.split(",");
        for (int i = 0; i<fieldArr.length; i++) {
            String field = fieldArr[i];
            //判断当前字段是否为主键字段
            if (pk.equals(field)) {
                createSql.append(field).append(" varchar primary key ");
            }else{
                createSql.append("info.").append(" varchar ");
            }
            if(i < fieldArr.length -1){
                createSql.append(",");
            }
        }
        createSql.append(")");
        createSql.append(ext);

        System.out.println("创建Phoenix表的语句：" + createSql);
        PreparedStatement ps = null;
        //获取Phoenix链接
        try {
             ps = conn.prepareStatement(createSql.toString());
            ps.execute();

        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException("Phoenix建表失败！！！");
                }
            }
        }
    }

    //每过来一个元素 方法执行一次 主要任务是根据内存中配置表Map 对当前进来的元素进行分流处理
    @Override
    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
        String table = jsonObj.getString("table");
        String type = jsonObj.getString("type");
        //注意：问题修复 如果使用Maxwell bootstrap同步历史数据， 这个时候操作类型叫bootstrap-insert
        if ("bootstrap-insert".equals(type)) {
            type = "insert";
            jsonObj.put("type",type);
        }
        //从内存中获取配置信息
        if(tableProcessMap != null && tableProcessMap.size()>0){
            //根据表名和操作类型拼接key
            String key = table + ":" +type;

            TableProcess tableProcess = tableProcessMap.get(key);

            //如果获取到了该元素对应的配置信息
            if (tableProcess != null) {
                //获取sinkTable, 指明这条数据应该发往何处
                //如果是维度数据，那么对应的是Phoenix中的表名
                jsonObj.put("sink_table",tableProcess.getSinkTable());
                String sinkColumns = tableProcess.getSinkColumns();
                //如果指定了sinkColumn,需要对保留的字段进行过滤处理
                if (sinkColumns != null && sinkColumns.length()>0) {
                    filterColumn(jsonObj.getJSONObject("data"),sinkColumns);
                }
            }else{
                System.out.println("No this Key:" + key + " in Mysql");
            }
            //根据sinkType将数据输出到不同的流
            if (tableProcess != null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)) {
                //如果sinktype = Hbase 通过侧输出流输出维度数据
                ctx.output(outputTag,jsonObj);
            }else if(tableProcess != null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_KAFKA)){
                //如果sinktype = kafka 通过主流输出 事实数据
                out.collect(jsonObj);
            }
        }
    }

    //对data中的数据进行过滤
    private void filterColumn(JSONObject data, String sinkColumns) {
        //sinkColums 保留列
        String[] cols = sinkColumns.split(",");
        //为了判断是否包含某个元素，将数值转换集合
        List<String> columnList = Arrays.asList(cols);

        //获取json对象中封装的键值对 每个键值对封装为ENTRY类型
        Set<Map.Entry<String, Object>> entrySet = data.entrySet();
        //集合转成迭代器，定义一个迭代器
        Iterator<Map.Entry<String, Object>> it = entrySet.iterator();
        //迭代器删除
        for (;it.hasNext();) {
            Map.Entry<String, Object> entry = it.next();
            if (!columnList.contains(entry.getKey())) {
                it.remove();
            }
        }
    }
}
