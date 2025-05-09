package com.lzr.realtime.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lzr.conf.constant.Constant;
import com.lzr.conf.utils.DateFormatUtil;
import com.lzr.conf.utils.FlinkSinkUtil;
import com.lzr.conf.utils.FlinkSourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

/**
 * @Package com.lzr.stream.realtime.com.lzy.stream.realtime.com.lzy.stream.realtime.v2.app.bwd.DwdBaseLog
 * @Author lzr
 * @Date 2025/4/11 10:35
 * @description: DwdBaseLog 日志表
 */


//DwdBaseLog主要功能是对用户行为日志进行清洗、修正和分流，将不同类型的日志数据分发到对应的 Kafka 主题中
public class DwdBaseLog {

    private static final String START = "start";
    private static final String ERR = "err";
    private static final String DISPLAY = "display";
    private static final String ACTION = "action";
    private static final String PAGE = "page";

    public static void main(String[] args) throws Exception {
//        创建一个流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        设置程序的默认并行度为4如果是本地运行就是4个线程，集群中就是4个任务槽
        env.setParallelism(4);
//        启用检查点机制，每5000毫秒(5秒)做一次检查点 确保每条记录只会被处理一次
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        创建一个 Kafka 数据源从FlinkSourceUtil读取kafka日志数据组id是dwd_log
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_LOG, "dwd_log");
//        从 Kafka 源创建一个数据流,使用 fromSource 方法将 KafkaSource 添加到执行环境中
        DataStreamSource<String> kafkaStrDS = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

//        kafkaStrDS.print();
//        定义一个侧输出标签，用于标记不符合要求的数据（如 JSON 解析失败的数据）。
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};
//       对Kafka 消费的字符串数据流应用进行逐条处理
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
//                            异常处理
                        } catch (Exception e) {
                            ctx.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );

//        jsonObjDS.print();

        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDS.sinkTo(FlinkSinkUtil.getKafkaSink("dirty_data"));
        //状态管理按mid分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<String> valueStateDescriptor =
                                new ValueStateDescriptor<>("lastVisitDateState", String.class);

                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build());

                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        String lastVisitDate = lastVisitDateState.value();
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);

                        if ("1".equals(isNew)) {
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                lastVisitDateState.update(curVisitDate);
                            } else {
                                if (!lastVisitDate.equals(curVisitDate)) {
                                    isNew = "0";
                                    jsonObj.getJSONObject("common").put("is_new", isNew);
                                }
                            }
                        } else {
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                String yesterDay = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                                lastVisitDateState.update(yesterDay);
                            }
                        }

                        return jsonObj;
                    }
                }
        );
//        fixedDS.print();

        // 定义侧输出流标签日志分流
        // 错误
        OutputTag<String> errTag = new OutputTag<String>("errTag") {};
        // 启动
        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
        // 曝光
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
        // 用户行为
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};
        //分流
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
             // 清洗和修正json日志数据流
//                根据日志内容特征，将其分类为五类数据流：
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) {
                        //~~~错误日志~~~
                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
//                        判断是否存在 err 字段。如果存在，将完整日志发送到错误流（errTag），并移除已处理的 err 字段。
                        if (errJsonObj != null) {
                            ctx.output(errTag, jsonObj.toJSONString());
                            jsonObj.remove("err");
                        }
//                        检查是否存在 start 字段（标记应用启动事件）。如果存在，将日志发送到启动流（startTag）。
                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        if (startJsonObj != null) {
                            ctx.output(startTag, jsonObj.toJSONString());
//                            如果日志不是启动日志则处理以下三类数据
                        } else {
                            //~~~页面日志~~~
                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");
                            //~~~曝光日志~~~
//                            检查 displays 数组（记录商品曝光信息）。为每条曝光记录构建新的标准化 JSON 对象，发送到曝光流
                            JSONArray displayArr = jsonObj.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject dispalyJsonObj = displayArr.getJSONObject(i);
                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    newDisplayJsonObj.put("common", commonJsonObj); // 通用字段
                                    newDisplayJsonObj.put("page", pageJsonObj); // 页面字段
                                    newDisplayJsonObj.put("display", dispalyJsonObj); // 曝光详情
                                    newDisplayJsonObj.put("ts", ts); // 时间戳
                                    ctx.output(displayTag, newDisplayJsonObj.toJSONString()); // 发送到曝光流
                                }
                                jsonObj.remove("displays");
                            }

                            //~~~用户行为日志~~~
//                            检查 actions 数组（记录用户点击、滑动等行为，为每条行为记录构建标准化 JSON 对象。发送到行为流
                            JSONArray actionArr = jsonObj.getJSONArray("actions");
                            if (actionArr != null && actionArr.size() > 0) {
                                for (int i = 0; i < actionArr.size(); i++) {
                                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common", commonJsonObj);
                                    newActionJsonObj.put("page", pageJsonObj);
                                    newActionJsonObj.put("action", actionJsonObj);
                                    ctx.output(actionTag, newActionJsonObj.toJSONString());
                                }
                                jsonObj.remove("actions");
                            }
//                            页面日志处理（主输出流）
                            out.collect(jsonObj.toJSONString());// 发送到页面日志流
                        }
                    }
                }
        );
//        从主数据流 pageDS 中提取之前通过 ctx.output() 分流的四种侧输出流：
        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);
//        将主流和侧输出流的内容打印到控制台，用于调试和验证数据是否正确分流。
        pageDS.print("页面:");
        errDS.print("错误:");
        startDS.print("启动:");
        displayDS.print("曝光:");
        actionDS.print("用户行为:");
//        3. 构建分流映射表
//        讲五个类数据包括主流存入map方便统一管理
        Map<String, DataStream<String>> streamMap = new HashMap<>();
        streamMap.put(ERR,errDS);
        streamMap.put(START,startDS);
        streamMap.put(DISPLAY,displayDS);
        streamMap.put(ACTION,actionDS);
        streamMap.put(PAGE,pageDS);

//        将不同类型的数据写入不同的kafka主题
        streamMap
                .get(PAGE)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        streamMap
                .get(ERR)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        streamMap
                .get(START)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        streamMap
                .get(DISPLAY)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        streamMap
                .get(ACTION)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));

        env.execute("dwd_log");
    }



}
