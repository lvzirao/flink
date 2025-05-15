package com.lzr.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Package com.lzr.domain.DimSkuInfoMsg
 * @Author lv.zirao
 * @Date 2025/5/15 15:45
 * @description:
 */

@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimSkuInfoMsg implements Serializable {
    private String id1;
    private String id2;
    private String category3_id;
    private String tm_name;
}