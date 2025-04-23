package com.lzr.realtime.dwd.test;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;
/**
 * @Package com.lzr.realtime.dwd.test.Test01_OutputTag
 * @Author lv.zirao
 * @Date 2025/4/14 10:45
 * @description:
 * 该案例演示了侧输出流标签创建问题
 */
public class Test01_OutputTag {
    public static void main(String[] args) {
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag", TypeInformation.of(String.class));
    }
}
