package com.lzr.conf.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
/**
 * @Package com.lzr.conf.bean.TrafficUvCt
 * @Author lv.zirao
 * @Date 2025/4/23 20:32
 * @description:
 */
@Data
@AllArgsConstructor
public class TrafficUvCt {
    // 渠道
    String ch;
    // 独立访客数
    Integer uvCt;
}
