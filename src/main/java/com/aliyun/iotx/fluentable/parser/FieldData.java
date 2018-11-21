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
public class FieldData {

    /**
     * Field whether present TableStorePrimaryKey
     */
    private boolean primaryKey;

    /**
     * field name
     */
    private String name;

    /**
     * field value convert from TableStore latest column
     */
    private Object value;

    /**
     * timestamp - field value
     */
    private Map<Long, Object> versions;
}
