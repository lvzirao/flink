package com.lzr.realtime.dwd.db.app;

import com.lzr.realtime.base.BaseSQLApp;
import com.lzr.realtime.constant.Constant;
import com.lzr.realtime.util.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.lzr.realtime.dwd.db.app.DwdInteractionCommentinfo
 * @Author lv.zirao
 * @Date 2025/4/15 16:44
 * @description:
 * 评论事实表
 */
public class DwdInteractionCommentinfo extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionCommentinfo().start(10012,4,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        // TODO 从kafka的topic_db主题中读取数据 创建动态表       ---kafka连接器
        readOdsDb(tableEnv,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
        // TODO 过滤出评论数据                                 ---where
        Table commentInfo = tableEnv.sqlQuery("select \n" +
                " data['id'] id,\n" +
                " data['user_id'] user_id,\n" +
                " data['sku_id'] sku_id,\n" +
                " data['appraise'] appraise,\n" +
                " data['comment_txt'] comment_txt,\n" +
                " ts,\n" +
                " proc_time\n" +
                " from topic_db where 'table'='comment_info' and 'type'='insert'");
//        commentInfo.execute().print();
        // 将表对象注册到表执行环境中
        tableEnv.createTemporaryView("comment_info",commentInfo);
        // TODO 从HBase中读取字典数据创建动态表                 ---hbase连接器
        readBaseDic(tableEnv);
        // TODO 评论表和字典表进行关联                        ---lookup join
        Table joinedTable = tableEnv.sqlQuery("SELECT\n" +
                " id,\n" +
                " user_id,\n" +
                " sku_id,\n" +
                " appraise,\n" +
                " dic.dic_name appraise_name,\n" +
                " comment_txt,\n" +
                " ts\n" +
                "FROM comment_info AS c\n" +
                "JOIN base_dic FOR SYSTEM_TIME AS OF c.proc_time AS dic\n" +
                "ON c.appraise = dic.dic_code");
        joinedTable.execute().print();
        // TODO 将表关联的结果写道kafka主题中                   ---upsert kafka连接器
        // 7.1 创建动态表和要写入的主题进行映射
        tableEnv.executeSql("CREATE TABLE "+Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO+" (\n" +
                " id string,\n" +
                " user_id string,\n" +
                " sku_id string,\n" +
                " appraise string,\n" +
                " appraise_name string,\n" +
                " comment_txt string,\n" +
                " ts bigint,\n" +
                " PRIMARY KEY (id) NOT ENFORCED\n" +
                ")"+SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        // 写入
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }
    private static void readOdsDb(StreamTableEnvironment tableEnv){
        tableEnv.executeSql("create table topic_db (\n" +
                " `database` string,\n" +
                " `table` string,\n" +
                " `type` string,\n" +
                " `ts` bigint,\n" +
                " `data` MAP<string,string>,\n" +
                " `old` MAP<string,string>,\n" +
                " `proc_time` as proctime()\n" +
                ")"+ SQLUtil.getKafkaDDL(Constant.TOPIC_DB,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
//        tableEnv.executeSql("select * from topic_db").print();
    }
    // 从hbase的字典表中读取数据，创建动态表
    public void readBaseDic(StreamTableEnvironment tableEnv){
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info RoW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ")"+SQLUtil.getHbaseDDL(":dim_base_dic"));
//        tableEnv.executeSql("select * from base_dic").print();
    }

































}
