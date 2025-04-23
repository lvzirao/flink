package com.lzr.conf.controller;


import com.lzr.conf.bean.TrafficUvCt;
import com.lzr.conf.service.TrafficStatsService;
import com.lzr.conf.util.DateFormatUtil;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
/**
 * @Package com.lzr.conf.controller.TrafficStatsController
 * @Author lv.zirao
 * @Date 2025/4/23 20:35
 * @description:
 */
public class TrafficStatsController {
    private TrafficStatsService trafficStatsService;

    public String getChUvCt(
            Integer date,
            Integer limit) {

        if (date == 0) {
            date = DateFormatUtil.now();
        }
        List<TrafficUvCt> trafficUvCtList = trafficStatsService.getChUvCt(date, limit);
        List chList = new ArrayList();
        List uvCtList = new ArrayList();
        for (TrafficUvCt trafficUvCt : trafficUvCtList) {
            chList.add(trafficUvCt.getCh());
            uvCtList.add(trafficUvCt.getUvCt());
        }


        String json = "{\"status\": 0,\"data\":{\"categories\": [\""+ StringUtils.join(chList,"\",\"")+"\"],\n" +
                "    \"series\": [{\"name\": \"渠道\",\"data\": ["+StringUtils.join(uvCtList,",")+"]}]}}";

        return json;
    }
}
