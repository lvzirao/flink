package com.lzr.conf.mapper;


import com.lzr.conf.bean.TrafficUvCt;
import io.lettuce.core.dynamic.annotation.Param;

import java.util.List;
/**
 * @Package com.lzr.conf.mapper.TrafficStatsMapper
 * @Author lv.zirao
 * @Date 2025/4/23 20:36
 * @description:
 */
public class TrafficStatsMapper {
    public List<TrafficUvCt> selectChUvCt(@Param("date") Integer date, @Param("limit") Integer limit) {
        return null;
    }
}
