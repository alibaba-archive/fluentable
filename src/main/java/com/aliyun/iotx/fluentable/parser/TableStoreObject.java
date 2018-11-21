package com.aliyun.iotx.fluentable.parser;

import java.util.Map;

import lombok.Getter;
import lombok.Setter;

/**
 * @author jiehong.jh
 * @date 2018/8/3
 */
@Getter
@Setter
public class TableStoreObject<T> {

    /**
     * Assembling with the latest values
     */
    private T object;

    /**
     * field name - < timestamp - field value >
     */
    private Map<String, Map<Long, Object>> detail;
}
