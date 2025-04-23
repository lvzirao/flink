package com.lzr.conf.service.impl;


import com.lzr.conf.bean.TrafficUvCt;
import com.lzr.conf.mapper.TrafficStatsMapper;
import com.lzr.conf.service.TrafficStatsService;

import java.util.List;

/**
 * @author Felix
 * @date 2024/6/14
 * 流量域统计service接口实现类
 */

public class TrafficStatsServiceImpl implements TrafficStatsService {

    private TrafficStatsMapper trafficStatsMapper;

    @Override
    public List<TrafficUvCt> getChUvCt(Integer date, Integer limit) {
        return trafficStatsMapper.selectChUvCt(date,limit);
    }
}
