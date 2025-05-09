package com.lzr.realtime.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.lzr.conf.bean.UserLoginBean;
import com.lzr.conf.function.BeanToJsonStrMapFunction;
import com.lzr.conf.utils.DateFormatUtil;
import com.lzr.conf.utils.FlinkSinkUtil;
import com.lzr.conf.utils.FlinkSourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Package com.lzr.stream.realtime.com.lzy.stream.realtime.com.lzy.stream.realtime.v2.app.dws.DwsUserUserLoginWindow
 * @Author lv.zirao
 * @Date 2025/4/21 9:56
 * @description: DwsUserUserLoginWindow
 * 业务含义：用户登录窗口聚合表
 * 分析维度：统计用户登录行为（如每日/每小时活跃用户数）
 * 典型指标：UV（独立用户数）、登录次数、登录设备分布
 */

public class DwsUserUserLoginWindow {
    public static void main(String[] args) throws Exception {
//        环境初始化与配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        并行度 全局设置为 1，简化调试。
        env.setParallelism(1);
//        检查点机制
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
//        数据摄入与预处理
//        从 Kafka 的dwd_traffic_page主题消费页面浏览数据。
//        将 JSON 字符串解析为JSONObject。
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_traffic_page", "dws_user_user_login_window");
        DataStreamSource<String> kafkaStrDS
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

//        jsonObjDS.print();
//        过滤登录事件用户 ID 非空
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        String uid = jsonObj.getJSONObject("common").getString("uid");
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        return StringUtils.isNotEmpty(uid)
                                && ("login".equals(lastPageId) || StringUtils.isEmpty(lastPageId));
                    }
                }
        );

//        filterDS.print();
//        水印与时间戳分配
//        水印策略：假设事件时间单调递增，使用forMonotonousTimestamps生成水印
//        时间戳提取：从 JSON 对象中提取ts字段作为事件时间。
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = filterDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts");
                                    }
                                }
                        )
        );

//        状态管理与用户行为分析
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("uid"));

        SingleOutputStreamOperator<UserLoginBean> beanDS = keyedDS.process(
//                状态管理：使用ValueState存储每个用户的最后登录日期。
                new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
                    private ValueState<String> lastLoginDateState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<>("lastLoginDateState", String.class);
                        lastLoginDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context ctx, Collector<UserLoginBean> out) throws Exception {

                        String lastLoginDate = lastLoginDateState.value();

                        Long ts = jsonObj.getLong("ts");
                        String curLoginDate = DateFormatUtil.tsToDate(ts);
                        // 首次登录：若状态为空，标记为今日首次登录uuCt=1，若上次登录日期与当前日期不同，标记为今日首次登录。若上次登录日期与当前日期间隔≥8 天，标记为回流用户backCt=1
                        //
                        long uuCt = 0L; // 独立用户登录数
                        long backCt = 0L; // 回流用户数
                        if (StringUtils.isNotEmpty(lastLoginDate)) {
                            if (!lastLoginDate.equals(curLoginDate)) {
                                uuCt = 1L;
                                lastLoginDateState.update(curLoginDate);
                                long day = (ts - DateFormatUtil.dateToTs(lastLoginDate)) / 1000 / 60 / 60 / 24;
                                if (day >= 8) {
                                    backCt = 1L;
                                }
                            }
                        } else {
                            uuCt = 1L;
                            lastLoginDateState.update(curLoginDate);
                        }

                        if (uuCt != 0L) {
                            out.collect(new UserLoginBean("", "", "", backCt, uuCt, ts));
                        }
                    }
                }
        );

//        beanDS.print();
//        窗口聚合与结果处理
//        滚动窗口：使用 10 秒的滚动事件时间窗口。
        AllWindowedStream<UserLoginBean, TimeWindow> windowDS = beanDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<UserLoginBean> reduceDS = windowDS.reduce(
//                ReduceFunction：累加窗口内的uuCt和backCt。
                new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) {
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        return value1;
                    }
                },
//                AllWindowFunction：设置窗口的开始时间stt、结束时间edt和日期curDate
                new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) {
                        UserLoginBean bean = values.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setCurDate(curDate);
                        out.collect(bean);
                    }
                }
        );
        // 打印
//        将UserLoginBean转换为 JSON 字符串
        SingleOutputStreamOperator<String> jsonMap = reduceDS
                .map(new BeanToJsonStrMapFunction<>());

        jsonMap.print();

//        jsonMap.sinkTo(FlinkSinkUtil.getDorisSink("dws_user_user_login_window"));

        env.execute();
    }
}
