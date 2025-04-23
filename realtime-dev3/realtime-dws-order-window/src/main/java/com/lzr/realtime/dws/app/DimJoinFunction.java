package com.lzr.realtime.dws.app;
import com.alibaba.fastjson.JSONObject;
/**
 * @Package com.lzr.realtime.dws.app.DimJoinFunction
 * @Author lv.zirao
 * @Date 2025/4/22 19:53
 * @description:
 */
public interface DimJoinFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj) ;

    String getTableName() ;

    String getRowKey(T obj) ;
}

