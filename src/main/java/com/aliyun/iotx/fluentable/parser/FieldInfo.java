package com.aliyun.iotx.fluentable.parser;

import java.lang.reflect.Field;

import com.aliyun.iotx.fluentable.converter.TableStoreConverter;
import com.aliyun.iotx.fluentable.formatter.TableStoreFormatter;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author jiehong.jh
 * @date 2018/8/2
 */
@Getter
@AllArgsConstructor
public class FieldInfo {
    private Field field;

    private String columnName;
    private Class<? extends TableStoreConverter> converterClazz;
    private Class<? extends TableStoreFormatter> formatterClazz;

    private boolean primaryKey;
    /**
     * 主键是否为自增
     */
    private boolean autoIncrement;
    /**
     * 只有主键需要关注顺序，普通列为null
     */
    private Integer order;
    private TableStoreType columnEnum;
}
