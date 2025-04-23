package com.lzr.realtime.dws.function;

import com.lzr.realtime.dws.util.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Package com.lzr.realtime.dws.function.KeywordUDTF
 * @Author lv.zirao
 * @Date 2025/4/17 21:35
 * @description:
 */
@FunctionHint(output =@DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String text){
        for (String keyword : KeyWordUtil.analyze(text)){
            // use collect(...) to emit a row
            collect(Row.of(keyword));
        }
    }






}
