package com.lzr.dmp.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lzr.conf.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.TimeZone;

/**
 * @Package com.lzr.dmp.dwd.DwdOrder
 * @Author lv.zirao
 * @Date 2025/5/14 19:21
 * @description: Flink订单处理，包含时间戳转换
 */
public class DwdOrder {
    public static void main(String[] args) throws Exception {
        // 1. 环境初始化与数据源配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("topic_dmp_db", "dwd_app");
        DataStreamSource<String> kafkaSource1 = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        SingleOutputStreamOperator<JSONObject> streamOperatorlog = kafkaSource1.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts_ms");
                            }
                        }));

        // 过滤订单主表数据
        SingleOutputStreamOperator<JSONObject> orderDs = streamOperatorlog.filter(data ->
                data.getJSONObject("source").getString("table").equals("order_info"));

        // 过滤订单明细表数据
        SingleOutputStreamOperator<JSONObject> detailDs = streamOperatorlog.filter(data ->
                data.getJSONObject("source").getString("table").equals("order_detail"));

        // 处理订单主表数据，提取需要的字段并转换时间戳
        SingleOutputStreamOperator<JSONObject> orderProcessed = orderDs.process(new ProcessFunction<JSONObject, JSONObject>() {
            private transient SimpleDateFormat dateFormat;

            @Override
            public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                super.open(parameters);
                // 初始化日期格式化器，设置时区为UTC
                dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            }

            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject reduced = new JSONObject();
                JSONObject after = value.getJSONObject("after");
                reduced.put("order_id", after.getString("id"));
                reduced.put("user_id", after.getString("user_id"));
                reduced.put("total_amount", after.getString("total_amount"));

                // 转换时间戳为日期字符串
                long createTimeMs = after.getLongValue("create_time");
                Date createDate = new Date(createTimeMs);
                reduced.put("create_time", dateFormat.format(createDate));

                out.collect(reduced);
            }
        });

        // 处理订单明细表数据，提取需要的字段
        SingleOutputStreamOperator<JSONObject> detailProcessed = detailDs.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject reduced = new JSONObject();
                JSONObject after = value.getJSONObject("after");
                reduced.put("order_id", after.getString("order_id"));
                reduced.put("sku_id", after.getString("sku_id"));
                reduced.put("sku_name", after.getString("sku_name"));
                out.collect(reduced);
            }
        });

        // 对订单主表数据按order_id进行keyBy操作，并过滤掉order_id为null的数据
        KeyedStream<JSONObject, String> orderKeyed = orderProcessed
                .filter(data -> data.getString("order_id") != null)
                .keyBy(data -> data.getString("order_id"));

        // 对订单明细表数据按order_id进行keyBy操作，并过滤掉order_id为null的数据
        KeyedStream<JSONObject, String> detailKeyed = detailProcessed
                .filter(data -> data.getString("order_id") != null)
                .keyBy(data -> data.getString("order_id"));

        // 使用interval join连接订单主表和明细表数据
        SingleOutputStreamOperator<JSONObject> joinedStream = orderKeyed
                .intervalJoin(detailKeyed)
                .between(Time.minutes(-5), Time.minutes(5)) // 设置时间间隔为前后5分钟
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject order, JSONObject detail, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject merged = new JSONObject();
                        // 添加订单主表信息
                        merged.put("order_id", order.getString("order_id"));
                        merged.put("user_id", order.getString("user_id"));
                        merged.put("total_amount", order.getString("total_amount"));
                        merged.put("create_time", order.getString("create_time"));

                        // 添加订单明细表信息
                        merged.put("sku_id", detail.getString("sku_id"));
                        merged.put("sku_name", detail.getString("sku_name"));

                        out.collect(merged);
                    }
                });

        // 打印结果
//        joinedStream.print();






        env.execute();
    }
}