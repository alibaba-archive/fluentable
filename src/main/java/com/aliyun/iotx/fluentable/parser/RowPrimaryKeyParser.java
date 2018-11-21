package com.aliyun.iotx.fluentable.parser;

import java.lang.reflect.Field;

import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.Row;
import com.aliyun.iotx.fluentable.converter.TableStoreConverter;

/**
 * @author jiehong.jh
 * @date 2018/8/3
 */
public class RowPrimaryKeyParser implements RowParser {

    private ClassInfo classInfo;

    public RowPrimaryKeyParser(ClassInfo classInfo) {
        this.classInfo = classInfo;
    }

    @Override
    public FieldData parse(Row row, FieldInfo fieldInfo) {
        Field field = fieldInfo.getField();
        TableStoreType columnEnum = fieldInfo.getColumnEnum();
        PrimaryKeyColumn column = row.getPrimaryKey().getPrimaryKeyColumn(fieldInfo.getColumnName());
        Object columnValue = getColumnValue(column, columnEnum.getPrimaryKeyType());
        TableStoreConverter converter = classInfo.getConverter(fieldInfo.getConverterClazz());
        FieldData fieldData = new FieldData();
        fieldData.setPrimaryKey(true);
        fieldData.setName(field.getName());
        fieldData.setValue(converter.convert(columnEnum, columnValue, field.getType()));
        return fieldData;
    }

    private Object getColumnValue(PrimaryKeyColumn primaryKeyColumn, PrimaryKeyType columnType) {
        switch (columnType) {
            case STRING:
                return primaryKeyColumn.getValue().asString();
            case INTEGER:
                return primaryKeyColumn.getValue().asLong();
            case BINARY:
                return primaryKeyColumn.getValue().asBinary();
            default:
                throw new UnsupportedOperationException(columnType.name() + " type is not support.");
        }
    }

    @Override
    public FieldData parseLatest(Row row, FieldInfo fieldInfo) {
        return parse(row, fieldInfo);
    }
}
