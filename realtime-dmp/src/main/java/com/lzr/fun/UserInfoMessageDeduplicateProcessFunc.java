package com.lzr.fun;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.lzr.fun.UserInfoMessageDeduplicateProcessFunc
 * @Author lv.zirao
 * @Date 2025/5/14 9:10
 * @description:  userInfo 数据去重
 */
public class UserInfoMessageDeduplicateProcessFunc extends KeyedProcessFunction<Long, JSONObject,JSONObject> {
    private ValueState<Long>lastTsState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 定义状态TTL（1天过期）
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();
        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                "latestTs",
                Long.class
        );
        descriptor.enableTimeToLive(ttlConfig);
        lastTsState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(JSONObject jsonObject, KeyedProcessFunction<Long, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        Long currentTs = jsonObject.getLong("ts_ms");
        Long latestTs = lastTsState.value();

        // 进当前记录的ts_ms大于状态中的ts_ms时，输出并更新状态
        if (latestTs == null || currentTs > latestTs){
            collector.collect(jsonObject);
            lastTsState.update(currentTs);
        }
    }
}
