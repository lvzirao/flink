package com.lzr.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.lzr.realtime.bean.TableProcessDim
 * @Author lv.zirao
 * @Date 2025/4/10 20:54
 * @description:
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDim {
    // 来源表名
    String sourceTable;
    // 目标表名
    String sinkTable;
    // 输出字段
    String sinkColumns;
    // 数据到hbase的列族
    String sinkFamily;
    // sink到hbase的时候的主键字段
    String sinkRowKey;
    // 配置表操作类型
    String op;

}
