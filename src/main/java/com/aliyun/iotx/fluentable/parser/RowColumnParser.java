package com.aliyun.iotx.fluentable.parser;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.Row;
import com.aliyun.iotx.fluentable.converter.TableStoreConverter;

/**
 * @author jiehong.jh
 * @date 2018/8/3
 */
public class RowColumnParser implements RowParser {

    private ClassInfo classInfo;

    public RowColumnParser(ClassInfo classInfo) {
        this.classInfo = classInfo;
    }

    @Override
    public FieldData parse(Row row, FieldInfo fieldInfo) {
        Field field = fieldInfo.getField();
        List<Column> columnList = row.getColumn(fieldInfo.getColumnName());
        if (columnList.isEmpty()) {
            FieldData fieldData = new FieldData();
            fieldData.setName(field.getName());
            return fieldData;
        }
        TableStoreConverter converter = classInfo.getConverter(fieldInfo.getConverterClazz());
        TableStoreType columnEnum = fieldInfo.getColumnEnum();
        int size = columnList.size();
        FieldData fieldData = new FieldData();
        Map<Long, Object> versions = new HashMap<>(size);
        fieldData.setName(field.getName());
        fieldData.setVersions(versions);
        for (int i = 0; i < size; i++) {
            Column column = columnList.get(i);
            Object columnValue = getColumnValue(columnEnum.getColumnType(), column);
            Object fieldValue = converter.convert(columnEnum, columnValue, field.getType());
            versions.put(column.getTimestamp(), fieldValue);
            if (i == 0) {
                fieldData.setValue(fieldValue);
            }
        }
        return fieldData;
    }

    private Object getColumnValue(ColumnType columnType, Column column) {
        switch (columnType) {
            case STRING:
                return column.getValue().asString();
            case DOUBLE:
                return column.getValue().asDouble();
            case BOOLEAN:
                return column.getValue().asBoolean();
            case INTEGER:
                return column.getValue().asLong();
            case BINARY:
                return column.getValue().asBinary();
            default:
                throw new UnsupportedOperationException(columnType.name() + " type is not support.");
        }
    }

    @Override
    public FieldData parseLatest(Row row, FieldInfo fieldInfo) {
        TableStoreType columnEnum = fieldInfo.getColumnEnum();
        Field field = fieldInfo.getField();
        FieldData fieldData = new FieldData();
        fieldData.setName(field.getName());
        Column column = row.getLatestColumn(fieldInfo.getColumnName());
        if (column != null) {
            TableStoreConverter converter = classInfo.getConverter(fieldInfo.getConverterClazz());
            Object columnValue = getColumnValue(columnEnum.getColumnType(), column);
            Object fieldValue = converter.convert(columnEnum, columnValue, field.getType());
            fieldData.setValue(fieldValue);
        }
        return fieldData;
    }
}
