package com.lzr.realtime.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.lzr.realtime.bean.TrafficPageViewBean;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Package com.lzr.realtime.function.BeanToJsonStrMapFunction
 * @Author lv.zirao
 * @Date 2025/4/21 16:28
 * @description:
 * 将流中对象转换为json格式字符串
 */
public class BeanToJsonStrMapFunction<T> implements MapFunction<T, String>{
    @Override
    public String map(T bean) throws Exception {
        SerializeConfig config = new SerializeConfig();
        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSON.toJSONString(bean, config);
    }
}
