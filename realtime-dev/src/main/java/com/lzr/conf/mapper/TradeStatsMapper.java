package com.lzr.conf.mapper;
import com.lzr.conf.bean.TradeProvinceOrderAmount;

import java.math.BigDecimal;
import java.util.List;
public interface TradeStatsMapper  {
    //获取某天总交易额
    BigDecimal selectGMV(Integer date);

    //获取某天各个省份交易额
    List<TradeProvinceOrderAmount> selectProvinceAmount(Integer date);
}
