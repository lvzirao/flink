package com.lzr.realtime.dwd.db.app;


import com.lzr.realtime.constant.Constant;
import com.lzr.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
/**
 * @Package com.lzr.realtime.dwd.db.app.DwdTradeCareAdd
 * @Author lv.zirao
 * @Date 2025/4/16 9:42
 * @description:
 * 加购事实表
 */
public class DwdTradeCareAdd{
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("create table topic_db(  " +
                "  `before` map<string,string>,  " +
                "  `after` map<string,string>,  " +
                "  `source` map<string,string>,  " +
                "  `op` string,  " +
                "  `ts_ms` BIGINT,  " +
                "  proc_time as proctime()  " +
                " ) "+  SQLUtil.getKafkaDDL("topic_db","one1"));

        Table cartInfo = tableEnv.sqlQuery("select \n" +
                "   `after`['id'] id,\n" +
                "   `after`['user_id'] user_id,\n" +
                "   `after`['sku_id'] sku_id,\n" +
                "   if(op='c',`after`['sku_num'], CAST((CAST(after['sku_num'] AS INT) - CAST(`before`['sku_num'] AS INT)) AS STRING)) sku_num, \n" +
                "   ts_ms\n" +
                "from topic_db \n" +
                "where `source`['table']='cart_info' \n" +
                "and (\n" +
                "    op = 'c'\n" +
                "    or\n" +
                "    (op='u' and `before`['sku_num'] is not null and (CAST(`after`['sku_num'] AS INT) >= CAST(`before`['sku_num'] AS INT)))\n" +
                ")");
//        cartInfo.execute().print();
        tableEnv.createTemporaryView("cartInfo",cartInfo);
        tableEnv.executeSql(" create table dwd_trade_cart_add(\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    sku_num string,\n" +
                "    ts_ms bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                " )" + SQLUtil.getUpsertKafkaDDL("dwd_trade_cart_add")  );
        //写入
        cartInfo.executeInsert("dwd_trade_cart_add");

    }
}
