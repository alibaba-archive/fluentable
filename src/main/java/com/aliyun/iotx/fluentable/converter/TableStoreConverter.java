package com.aliyun.iotx.fluentable.converter;

import com.aliyun.iotx.fluentable.parser.TableStoreType;

/**
 * @author jiehong.jh
 * @date 2018/8/2
 */
@FunctionalInterface
public interface TableStoreConverter {

    /**
     * convert TableStore column value to Java field value
     *
     * @param columnEnum
     * @param columnValue
     * @param fieldType
     * @return field value
     */
    Object convert(TableStoreType columnEnum, Object columnValue, Class<?> fieldType);
}
