package com.lzr.dmp.dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lzr.conf.utils.FlinkSourceUtil;
import com.lzr.fun.ProcessJoinBase2And4BaseFunc;
import com.lzr.fun.ProcessLabelFunc;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @Package com.lzr.dmp.dwd.DwsLable6
 * @Author lv.zirao
 * @Date 2025/5/15 18:16
 * @description:
 */
public class dwd {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> kafkaSource_base6 = FlinkSourceUtil.getKafkaSource("kafka_label_base6_topic", "dwd_app_6");
        DataStreamSource<String> kafkaSource6 = env.fromSource(kafkaSource_base6, WatermarkStrategy.noWatermarks(), "kafka_source6");

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
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts_ms");
                            }
                        }));
//        kafka_base4.print("kafka_base4");

        KafkaSource<String> kafkaSource_base2 = FlinkSourceUtil.getKafkaSource("kafka_label_base2_topic", "dwd_app_2");
        DataStreamSource<String> kafkaSource2 = env.fromSource(kafkaSource_base2, WatermarkStrategy.noWatermarks(), "kafka_source2");
        SingleOutputStreamOperator<JSONObject> kafka_base2 = kafkaSource2.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                // 检查 "ts_ms" 字段是否存在且不为 null
                                if (element.containsKey("ts_ms") && element.get("ts_ms") != null) {
                                    return element.getLongValue("ts_ms"); // 使用 getLongValue 避免 null
                                } else {
                                    // 若字段不存在或为 null，使用当前处理时间或其他默认值
//                                    System.err.println("Missing 'ts_ms' field in record: " + element);
                                    return recordTimestamp; // 或者 System.currentTimeMillis()
                                }
                            }
                        })
                );
//        kafka_base2.print("kafka_base2");

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
                .process(new ProcessLabelFunc());
        userLabelProcessDs.print("userLabelProcessDs");





        env.execute();

    }
}
