package com.lzr.conf.service.impl;



import com.lzr.conf.bean.TradeProvinceOrderAmount;
import com.lzr.conf.mapper.TradeStatsMapper;
import com.lzr.conf.service.TradeStatsService;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author Felix
 * @date 2024/6/14
 * 交易域统计service接口实现类
 */

public class TradeStatsServiceImpl implements TradeStatsService {

    private TradeStatsMapper tradeStatsMapper;
    @Override
    public BigDecimal getGMV(Integer date) {
        return tradeStatsMapper.selectGMV(date);
    }

    @Override
    public List<TradeProvinceOrderAmount> getProvinceAmount(Integer date) {
        return tradeStatsMapper.selectProvinceAmount(date);
    }
}
