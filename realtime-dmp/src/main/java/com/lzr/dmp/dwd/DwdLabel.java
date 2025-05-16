package com.lzr.dmp.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lzr.conf.utils.FlinkSourceUtil;
import com.lzr.fun.ProcessJoinBase2And4BaseFunc;
import com.lzr.fun.ProcessLabelFunc;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @Package com.lzr.dmp.dwd.DwdLabel
 * @Author lv.zirao
 * @Date 2025/5/15 19:51
 * @description:
 */
public class DwdLabel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        创建 Flink 执行环境并设置并行度为 1，确保所有操作在单个并行实例中执行
        env.setParallelism(1);

        KafkaSource<String> kafkaSource_base6 = FlinkSourceUtil.getKafkaSource("kafka_label_base6_topic", "dwd_app_6");
        DataStreamSource<String> kafkaSource6 = env.fromSource(kafkaSource_base6, WatermarkStrategy.noWatermarks(), "kafka_source6");
//        数据解析与时间戳分配 将 JSON 字符串解析为 JSONObject
        SingleOutputStreamOperator<JSONObject> kafka_base6 = kafkaSource6.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts_ms");
                            }
                        }));
//        kafka_base6.print("kafka_base6");

        KafkaSource<String> kafkaSource_base4 = FlinkSourceUtil.getKafkaSource("kafka_label_base4_topic", "dwd_app_4");
        DataStreamSource<String> kafkaSource4 = env.fromSource(kafkaSource_base4, WatermarkStrategy.noWatermarks(), "kafka_source4");
        SingleOutputStreamOperator<JSONObject> kafka_base4 = kafkaSource4.map(JSON::parseObject)
//        设置 5 秒的乱序
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
//                                使用数据中的ts_ms字段作为事件时间戳
                                return element.getLong("ts_ms");
                            }
                        }));
//        kafka_base4.print("kafka_base4");

        KafkaSource<String> kafkaSource_base2 = FlinkSourceUtil.getKafkaSource("kafka_label_base2_topic", "dwd_app_2");
        DataStreamSource<String> kafkaSource2 = env.fromSource(kafkaSource_base4, WatermarkStrategy.noWatermarks(), "kafka_source2");
        SingleOutputStreamOperator<JSONObject> kafka_base2 = kafkaSource2.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts_ms");
                            }
                        }));
//        kafka_base2.print("kafka_base2");

//        第一次流关联操作 目的是将具有相同 uid 的数据分到同一个分组
        SingleOutputStreamOperator<JSONObject> join2_4Ds = kafka_base2.keyBy(o -> o.getString("uid"))
                .intervalJoin(kafka_base4.keyBy(o -> o.getString("uid")))
                .between(Time.minutes(-2), Time.minutes(2))
                .process(new ProcessJoinBase2And4BaseFunc());
//        join2_4Ds.print("join2_4Ds");

        SingleOutputStreamOperator<JSONObject> waterJoin2_4 = join2_4Ds.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (jsonObject, l) -> jsonObject.getLongValue("ts_ms")));
//        waterJoin2_4.print("waterJoin2_4");


        SingleOutputStreamOperator<JSONObject> userLabelProcessDs = waterJoin2_4.keyBy(o -> o.getString("uid"))
                .intervalJoin(kafka_base6.keyBy(o -> o.getString("uid")))
                .between(Time.minutes(-2), Time.minutes(2))
                .process(new ProcessLabelFunc()); // 使用自定义的ProcessLabelFunc
        userLabelProcessDs.print();

// 写入文件
        userLabelProcessDs
                .map(JSONObject::toString)  // 将JSONObject转为字符串
                .writeAsText("D:\\idea\\flink\\docs\\lzr\\userLabelProcessDs.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);  // 设置为1避免生成多个分片文件
        env.execute();
    }
}
