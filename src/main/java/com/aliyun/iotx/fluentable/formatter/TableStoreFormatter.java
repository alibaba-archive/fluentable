package com.aliyun.iotx.fluentable.formatter;

import com.aliyun.iotx.fluentable.model.OptionalColumnValue;
import com.aliyun.iotx.fluentable.parser.TableStoreType;

/**
 * @author jiehong.jh
 * @date 2018/9/7
 */
@FunctionalInterface
public interface TableStoreFormatter {

    /**
     * format Java field value to TableStore column value
     *
     * @param columnEnum
     * @param fieldValue
     * @param fieldType
     * @return column value, Null is not allowed.
     */
    OptionalColumnValue format(TableStoreType columnEnum, Object fieldValue, Class<?> fieldType);
}
