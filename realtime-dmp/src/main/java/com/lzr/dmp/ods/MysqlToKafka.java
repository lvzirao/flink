package com.lzr.dmp.ods;

import com.lzr.conf.utils.FlinkSinkUtil;
import com.lzr.conf.utils.FlinkSourceUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @Package com.lzr.retail.com.lzy.stream.realtime.v1.realtime.app.ods.MysqlToKafka
 * @Author lv.zirao
 * @Date 2025/5/12 22:25
 * @description: MysqlToKafka
 */

public class MysqlToKafka {
    public static void main(String[] args) throws Exception {
//       创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//       设置并行度
        env.setParallelism(1);
//       获取数据源
        MySqlSource<String> realtimeV1 = FlinkSourceUtil.getMySqlSource("realtime_dmp", "*");
        DataStreamSource<String> mySQLSource = env.fromSource(realtimeV1, WatermarkStrategy.noWatermarks(), "MySQL Source");

        mySQLSource.print();

        KafkaSink<String> topic_dmp_db = FlinkSinkUtil.getKafkaSink("topic_dmp_db");

        mySQLSource.sinkTo(topic_dmp_db);

        env.execute("Print MySQL Snapshot + Binlog");

    }
}
