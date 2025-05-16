package com.lzr.fun;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;

/**
 * @Package com.lzr.fun.ProcessJoinBase2And4BaseFunc
 * @Author lv.zirao
 * @Date 2025/5/15 19:30
 * @description:
 */
//继承ProcessJoinFunction，用于双流连接处理
public class ProcessJoinBase2And4BaseFunc extends ProcessJoinFunction<JSONObject, JSONObject, JSONObject> {
//    类定义与成员变量
    private static final String[] list2 = {"18_24", "25_29", "30_34", "35_39", "40_49", "50"};

    private static final String[] list4 = {"18-24", "25-29", "30-34", "35-39", "40-49", "50"};


    @Override
    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        // 提取右侧流中的搜索权重
        JSONObject searchWeight = new JSONObject();
        for (String s : list2) {
            double v = right.getDoubleValue("search_" + s);
            searchWeight.put(s, v);
        }
// 提取设备权重
        JSONObject deviceWeight = new JSONObject();
        for (String s : list2) {
            double v = right.getDoubleValue("device_" + s);
            deviceWeight.put(s, v);
        }
        // 提取支付时间权重（处理分隔符差异）
        JSONObject payTimeWeight = new JSONObject();
        for (String s : list4) {
            double v = right.getDoubleValue("pay_time_" + s);
            String s1 = s.replace("-", "_");
            payTimeWeight.put(s1, v);
        }
// 提取b1name权重
        JSONObject b1nameWeight = new JSONObject();
        for (String s : list4) {
            double v = right.getDoubleValue("b1name_" + s);
            String s1 = s.replace("-", "_");
            b1nameWeight.put(s1, v);
        }
// 提取tname权重
        JSONObject tnameWeight = new JSONObject();
        for (String s : list4) {
            double v = right.getDoubleValue("tname_" + s);
            String s1 = s.replace("-", "_");
            tnameWeight.put(s1, v);
        }
// 提取amount权重
        JSONObject amountWeight = new JSONObject();
        for (String s : list4) {
            double v = right.getDoubleValue("amount_" + s);
            String s1 = s.replace("-", "_");
            amountWeight.put(s1, v);
        }

// 构建结果对象，提取用户ID和时间戳
        JSONObject result = new JSONObject();
        String uid = right.getString("uid");
        String ts_ms = right.getString("ts_ms");
        result.put("uid",uid);
        result.put("ts_ms",ts_ms);
        // 计算最终年龄层级并输出
        String ageLevel = getAgeLevel(searchWeight, deviceWeight, payTimeWeight, b1nameWeight, tnameWeight, amountWeight);
        result.put("ageLevel",ageLevel);
        collector.collect(result);

    }
    public static String getAgeLevel(JSONObject search,JSONObject device,JSONObject payTime,JSONObject b1name,JSONObject tname, JSONObject amount){
        ArrayList<Double> codes = new ArrayList<>();
        for (String s : list2) {
            Double v=search.getDoubleValue(s)
                    + device.getDoubleValue(s)
                    + payTime.getDoubleValue(s)
                    + b1name.getDoubleValue(s)
                    + tname.getDoubleValue(s)
                    + amount.getDoubleValue(s);
            codes.add(v);
        }
        Double max = Collections.max(codes);
        int i = codes.indexOf(max);
        return list2[i];


    }
}
