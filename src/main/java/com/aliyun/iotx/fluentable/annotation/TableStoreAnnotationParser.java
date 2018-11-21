package com.aliyun.iotx.fluentable.annotation;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alicloud.openservices.tablestore.model.Row;
import com.aliyun.iotx.fluentable.model.OptionalColumn;
import com.aliyun.iotx.fluentable.model.OptionalColumnValue;
import com.aliyun.iotx.fluentable.model.OptionalRow;
import com.aliyun.iotx.fluentable.model.SortedPrimaryKey;
import com.aliyun.iotx.fluentable.parser.ClassInfo;
import com.aliyun.iotx.fluentable.parser.FieldData;
import com.aliyun.iotx.fluentable.parser.FieldInfo;
import com.aliyun.iotx.fluentable.parser.RowColumnParser;
import com.aliyun.iotx.fluentable.parser.RowPrimaryKeyParser;
import com.aliyun.iotx.fluentable.parser.TableStoreObject;
import com.aliyun.iotx.fluentable.util.TableStorePkBuilder;

/**
 * @author jiehong.jh
 * @date 2018/8/2
 */
public class TableStoreAnnotationParser {

    private ClassInfo classInfo;
    private RowColumnParser rowColumnParser;
    private RowPrimaryKeyParser rowPrimaryKeyParser;

    public TableStoreAnnotationParser(ClassInfo classInfo) {
        this.classInfo = classInfo;
        this.rowColumnParser = new RowColumnParser(classInfo);
        this.rowPrimaryKeyParser = new RowPrimaryKeyParser(classInfo);
    }

    /**
     * orm: parse TableStore row latest column to Java object
     *
     * @param row
     * @param object
     * @param <T>
     * @return
     */
    public <T> void parseInto(Row row, T object) {
        if (row == null || object == null) {
            return;
        }
        List<FieldInfo> fields = classInfo.getFields(object.getClass());
        for (FieldInfo fieldInfo : fields) {
            FieldData fieldData;
            if (fieldInfo.isPrimaryKey()) {
                fieldData = rowPrimaryKeyParser.parseLatest(row, fieldInfo);
            } else {
                fieldData = rowColumnParser.parseLatest(row, fieldInfo);
            }
            // 没有查询该列或者该列不存在
            Object value = fieldData.getValue();
            if (value != null) {
                setField(object, fieldInfo.getField(), value);
            }
        }
    }

    /**
     * orm: parse TableStore row latest column to Java object
     *
     * @param row
     * @param clazz
     * @param <T>
     * @return
     */
    public <T> T parseLatest(Row row, Class<T> clazz) {
        if (row == null) {
            return null;
        }
        T object = newInstance(clazz);
        parseInto(row, object);
        return object;
    }

    /**
     * orm: parse TableStore row to Java object
     *
     * @param row
     * @param clazz
     * @param <T>
     * @return
     */
    public <T> TableStoreObject<T> parse(Row row, Class<T> clazz) {
        if (row == null) {
            return new TableStoreObject<>();
        }
        TableStoreObject<T> tableStoreObject = new TableStoreObject<>();
        T object = newInstance(clazz);
        tableStoreObject.setObject(object);
        List<FieldInfo> fields = classInfo.getFields(clazz);
        Map<String, Map<Long, Object>> detail = new HashMap<>(fields.size());
        tableStoreObject.setDetail(detail);
        for (FieldInfo fieldInfo : fields) {
            FieldData fieldData;
            if (fieldInfo.isPrimaryKey()) {
                fieldData = rowPrimaryKeyParser.parse(row, fieldInfo);
            } else {
                fieldData = rowColumnParser.parse(row, fieldInfo);
            }
            Object value = fieldData.getValue();
            if (value != null) {
                setField(object, fieldInfo.getField(), value);
            }
            detail.put(fieldData.getName(), fieldData.getVersions());
        }
        return tableStoreObject;
    }

    /**
     * orm: parse Java object to TableStore row
     *
     * @param object
     * @return
     */
    public OptionalRow parse(Object object) {
        return parse(object, true);
    }

    /**
     * orm: parse Java object to TableStore row
     *
     * @param object
     * @return
     */
    public OptionalRow parse(Object object, boolean enableAutoIncrement) {
        if (object == null) {
            return OptionalRow.empty();
        }
        List<FieldInfo> fields = classInfo.getFields(object.getClass());
        OptionalRow row = new OptionalRow(fields.size());
        List<SortedPrimaryKey> primaryKeys = new ArrayList<>();
        for (FieldInfo fieldInfo : fields) {
            String columnName = fieldInfo.getColumnName();
            if (enableAutoIncrement && fieldInfo.isAutoIncrement()) {
                primaryKeys.add(new SortedPrimaryKey(fieldInfo.getOrder(), columnName, true, null));
                continue;
            }
            Field field = fieldInfo.getField();
            Object fieldValue = getFieldValue(object, field);
            OptionalColumnValue columnValue = classInfo.getFormatter(fieldInfo.getFormatterClazz())
                .format(fieldInfo.getColumnEnum(), fieldValue, field.getType());
            if (fieldInfo.isPrimaryKey()) {
                if (!columnValue.isPresent()) {
                    throw new NullPointerException("the value of primary key [" + columnName + "] should not be null.");
                }
                primaryKeys.add(new SortedPrimaryKey(fieldInfo.getOrder(), columnName, false, columnValue.get()));
            } else {
                row.put(new OptionalColumn(columnName, columnValue));
            }
        }
        if (primaryKeys.isEmpty()) {
            throw new IllegalArgumentException("the primary key must exist");
        }
        Collections.sort(primaryKeys);
        TableStorePkBuilder pkBuilder = new TableStorePkBuilder();
        primaryKeys.forEach(pk -> {
            if (pk.isAutoIncrement()) {
                pkBuilder.addAuto(pk.getName());
            } else {
                pkBuilder.add(pk.getName(), pk.getValue());
            }
        });
        row.primaryKey(pkBuilder.build());
        return row;
    }

    private <T> T newInstance(Class<T> clazz) {
        try {
            return clazz.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private <T> void setField(T object, Field field, Object value) {
        try {
            field.set(object, value);
        } catch (Exception e) {
            // Unreachable
            throw new RuntimeException(e);
        }
    }

    private Object getFieldValue(Object obj, Field field) {
        try {
            return field.get(obj);
        } catch (IllegalAccessException e) {
            // Unreachable
            throw new RuntimeException(e);
        }
    }
}
