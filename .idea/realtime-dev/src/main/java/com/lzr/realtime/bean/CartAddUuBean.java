package com.lzr.realtime.bean;
import lombok.AllArgsConstructor;
import lombok.Data;
/**
 * @Package com.lzr.realtime.bean.CartAddUuBean
 * @Author lv.zirao
 * @Date 2025/4/22 13:36
 * @description:
 */
@Data
@AllArgsConstructor
public class CartAddUuBean {
    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    // 当天日期
    String curDate;
    // 加购独立用户数
    Long cartAddUuCt;
}
