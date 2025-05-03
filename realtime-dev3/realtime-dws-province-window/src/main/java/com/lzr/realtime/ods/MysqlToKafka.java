package com.lzr.realtime.ods;

import com.lzr.realtime.util.FlinkSinkUtil;
import com.lzr.realtime.util.FlinkSourceUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.lzr.realtime.ods.MysqlToKafka
 * @Author lv.zirao
 * @Date 2025/5/3 14:53
 * @description:
 */
public class MysqlToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MySqlSource<String> realtimeV1 = FlinkSourceUtil.getMysqlSource("realtime_v1", "*");

        DataStreamSource<String> mySQLSource = env.fromSource(realtimeV1, WatermarkStrategy.noWatermarks(), "MySQL Source");

//        mySQLSource.print();

        KafkaSink<String> topic_db = FlinkSinkUtil.getKafkaSink("topic_db");

        mySQLSource.sinkTo(topic_db);

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
