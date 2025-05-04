package com.lzr.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lzr.realtime.base.BaseApp;
import com.lzr.realtime.bean.TableProcessDim;
import com.lzr.realtime.constant.Constant;
import com.lzr.realtime.dim.function.HBaseSinkFunction;
import com.lzr.realtime.dim.function.TableProcessFunction;
import com.lzr.realtime.util.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import java.util.*;
/**
 * @Package com.lzr.realtime.dim.app.DimApp
 * @Author lv.zirao
 * @Date 2025/4/9 22:49
 * @description:
 */
public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DimApp().start(10001,4,"dim_app",Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 4.对业务流中数据类型进行转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector){

                        com.alibaba.fastjson.JSONObject jsonObj = JSON.parseObject(s);
                        String db = jsonObj.getJSONObject("source").getString("db");
                        String op = jsonObj.getString("op");
                        String after = jsonObj.getString("after");
                        if ("realtime_v1".equals(db)
                                && ("c".equals(op)
                                || "u".equals(op)
                                || "d".equals(op)
                                || "r".equals(op))
                                && after != null
                                && after.length() > 2
                        ) {
                            collector.collect(jsonObj);
                        }
                    }
                }
        );
//        jsonObjDS.print();
//        //TODO 5.使用cdc读取配置表中的配置信息
//        //5.1 创建mysqlSource对象
        Properties pro = new Properties();
        Properties props=new Properties();
        props.setProperty("useSSL","false");
        props.setProperty("allowPublicKeyRetrieval","true");
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList("realtime_v2")
                .tableList("realtime_v2.table_process_dim")
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(pro)
                .build();
        //5.2 读取数据封装为流
        DataStreamSource<String> mysqlStrDs = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");
//        mysqlStrDs.print();
//        //TODO 6.对配置流中的数据类型转换
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDs.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim  map(String jsonStr){
                        // 为了处理方便，先将jsonStr转换为jsonObj
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        String op = jsonObject.getString("op");
                        TableProcessDim tableProcessDim=null;
                        if ("d".equals(op)){
                            // 对配置表进行了一次删除操作 从before属性中获取删除前的配置信息
                            tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);
                        }else{
                            // 对配置表进行了读取、添加、修改操作 从  after属性中获取最新的配置信息
                            tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);
//        tpDS.print();
//        //TODO 7.根据配置表中的配置信息到HBase中执行建表或者删除表操作
        tpDS =tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    private Connection hbaseConn;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn=HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tp) throws Exception {
                        // 获取对配置表进行的操作的类型
                        String op=tp.getOp();
                        // 获取hbase中维度表的表名
                        String sinkTable = tp.getSinkTable();
                        // 获取hbase中建表的列族
                        String[] sinkFamilies = tp.getSinkFamily().split(",");
                        if("d".equals(op)){
                            // 从配置表中删除了一条数据 讲hbase中对应的表删除掉
                            HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                        }else if("r".equals(op)||"c".equals(op)){
                            // 从配置表中读取了一条数据或者象配置表中添加了一条数据  在hbase中执行建表
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }else{
                            // 对配置表中的配置信息进行了修改 先从hbase将对应的表删除掉，在创建表
                            HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }
                        return tp;
                    }
                }
        ).setParallelism(1);
//        tpDS.print();
//        //TODO 8.将配置流中信息进行广播流处理
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor
                = new MapStateDescriptor("mapStateDescriptor",String.class,TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);
//        //TODO 9.将主流业务数据和广播流配置信息进行关联
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);
//        //TODO 10.处理关联后的数据（判断是否为维度）
        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcessDim>> dimDS = connectDS.process(
                new TableProcessFunction(mapStateDescriptor)
        );
//TODO  11.将维度表数据传到Hbase中去
        //({"tm_name":"苹果15","op":"u","id":2},TableProcessDim(sourceTable=base_trademark, sinkTable=dim_base_trademark, sinkColumns=id,tm_name, sinkFamily=info, sinkRowKey=id, op=c))
        dimDS.addSink(new HBaseSinkFunction());

    }
}
