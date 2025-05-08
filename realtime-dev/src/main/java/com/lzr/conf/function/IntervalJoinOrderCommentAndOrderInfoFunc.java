package com.lzr.conf.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.lzr.conf.function.IntervalJoinOrderCommentAndOrderInfoFunc
 * @Author lv.zirao
 * @Date 2025/5/8 19:57
 * @description:
 */
public class IntervalJoinOrderCommentAndOrderInfoFunc extends ProcessJoinFunction<JSONObject,JSONObject,JSONObject> {
    @Override
    public void processElement(JSONObject comment, JSONObject info, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject enrichedComment = (JSONObject)comment.clone();

        for (String key : info.keySet()) {
            enrichedComment.put("info_" + key, info.get(key));
        }
        out.collect(enrichedComment);
    }
}
