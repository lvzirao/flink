package com.lzr.conf.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
/**
 * @Package com.lzr.conf.bean.TradeProvinceOrderAmount
 * @Author lv.zirao
 * @Date 2025/4/23 20:33
 * @description:
 */

@Data
@AllArgsConstructor
public class TradeProvinceOrderAmount {
    // 省份名称
    String provinceName;
    // 下单金额
    Double orderAmount;
}
