package com.lzr.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.lzr.realtime.bean.TableProcessDim;
import com.lzr.realtime.constant.Constant;
import com.lzr.realtime.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;

/**
 * @Package com.lzr.realtime.dim.function.TableProcessFunction
 * @Author lv.zirao
 * @Date 2025/4/11 16:29
 * @description:
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>> {
    private MapStateDescriptor<String,TableProcessDim>mapStateDescriptor;
    private Map<String,TableProcessDim> cofigMap = new HashMap<>();

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Connection mySQLConnection = JdbcUtil.getMySQLConnection();
        List<TableProcessDim> tableProcessDims = JdbcUtil.querList(mySQLConnection, "select * from gmall.table_process_dim", TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDims) {
            cofigMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        JdbcUtil.closeMySQLConnection(mySQLConnection);
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        //获取处理数据的表名
        String table = jsonObject.getJSONObject("source").getString("table");
        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = readOnlyctx.getBroadcastState(mapStateDescriptor);
        //根据表名到广播状态中获取对应的配置信息,如果没有找到对应的配置，再到congiMap中获取
        TableProcessDim tableProcessDim = null;
        if ((tableProcessDim=broadcastState.get(table)) != null
                ||(tableProcessDim=cofigMap.get(table)) != null) {
            //如果根据表名获取到对应的配置信息，说明当前处理的是维度数据，将维度数据唏嘘向下游传递
            JSONObject after = jsonObject.getJSONObject("after");
            //再向下游传递数据之前过滤掉不需要传递的属性
            String sinkColumns = tableProcessDim.getSinkColumns();
            deleteNotNeedColumns(after,sinkColumns);
            //在想下游传递数据前，应当补充维度数据的操作类型
            String op = jsonObject.getString("op");
            after.put("op",op);
            out.collect(Tuple2.of(after,tableProcessDim));
        }
    }

    @Override
    public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        String op = tp.getOp();
        //获取广播状态
        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //获取维度表名称
        String sourceTable = tp.getSourceTable();
        if (op.equals("d")) {
            //从配置表中删除一条数据，将对应的配置信息也从广播战中删除
            broadcastState.remove(sourceTable);
            cofigMap.remove(sourceTable);
        } else {
            //对配置表中进行了兑取、添加或读取，将最新的信息放到广播状态
            broadcastState.put(sourceTable, tp);
            cofigMap.put(sourceTable,tp);
        }
    }
    private static void deleteNotNeedColumns(JSONObject after, String sinkColumns) {
        List<String> list = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entries = after.entrySet();
        entries.removeIf(e -> !list.contains(e.getKey()));
    }
}
