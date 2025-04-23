package com.lzr.conf.controller;

import com.lzr.conf.bean.TradeProvinceOrderAmount;
import com.lzr.conf.service.TradeStatsService;
import com.lzr.conf.util.DateFormatUtil;

import java.math.BigDecimal;
import java.util.List;
/**
 * @Package com.lzr.conf.controller.TradeStatsController
 * @Author lv.zirao
 * @Date 2025/4/23 20:34
 * @description:
 */
public class TradeStatsController {
    private TradeStatsService tradeStatsService;
    public String getGMV(Integer date){
        if(date == 0){
            //说明请求的时候，没有传递日期参数，将当天日期作为查询的日期
            date = DateFormatUtil.now();
        }
        BigDecimal gmv = tradeStatsService.getGMV(date);
        String json = "{\n" +
                "\t\t  \"status\": 0,\n" +
                "\t\t  \"data\": "+gmv+"\n" +
                "\t\t}";
        return json;
    }

    public String getProvinceAmount(Integer date){
        if(date == 0){
            date = DateFormatUtil.now();
        }
        List<TradeProvinceOrderAmount> provinceOrderAmountList = tradeStatsService.getProvinceAmount(date);

        StringBuilder jsonB = new StringBuilder("{\"status\": 0,\"data\": {\"mapData\": [");
        for (int i = 0; i < provinceOrderAmountList.size(); i++) {
            TradeProvinceOrderAmount provinceOrderAmount = provinceOrderAmountList.get(i);
            jsonB.append("{\"name\": \""+provinceOrderAmount.getProvinceName()+"\",\"value\": "+provinceOrderAmount.getOrderAmount()+"}");
            if(i < provinceOrderAmountList.size() - 1){
                jsonB.append(",");
            }
        }

        jsonB.append("],\"valueName\": \"交易额\"}}");
        return jsonB.toString();
    }
}
