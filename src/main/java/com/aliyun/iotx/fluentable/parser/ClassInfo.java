package com.aliyun.iotx.fluentable.parser;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.aliyun.iotx.fluentable.annotation.TableStoreColumn;
import com.aliyun.iotx.fluentable.annotation.TableStorePrimaryKey;
import com.aliyun.iotx.fluentable.converter.DefaultConverter;
import com.aliyun.iotx.fluentable.converter.TableStoreConverter;
import com.aliyun.iotx.fluentable.formatter.DefaultFormatter;
import com.aliyun.iotx.fluentable.formatter.TableStoreFormatter;

/**
 * @author jiehong.jh
 * @date 2018/8/3
 */
public class ClassInfo {

    private final Map<Class<?>, List<FieldInfo>> fieldInfoCache = new HashMap<>();
    private final Map<Class<? extends TableStoreConverter>, TableStoreConverter> converterCache = new HashMap<>();
    private final Map<Class<? extends TableStoreFormatter>, TableStoreFormatter> formatterCache = new HashMap<>();

    public ClassInfo(List<TableStoreConverter> converters, List<TableStoreFormatter> formatters) {
        converters.forEach(converter -> converterCache.put(converter.getClass(), converter));
        formatters.forEach(formatter -> formatterCache.put(formatter.getClass(), formatter));
    }

    /**
     * get converter from clazz
     *
     * @param converterClazz
     * @return
     */
    public TableStoreConverter getConverter(Class<? extends TableStoreConverter> converterClazz) {
        return converterCache.get(converterClazz);
    }

    /**
     * get formatter from clazz
     *
     * @param formatterClazz
     * @return
     */
    public TableStoreFormatter getFormatter(Class<? extends TableStoreFormatter> formatterClazz) {
        return formatterCache.get(formatterClazz);
    }

    /**
     * get clazz fields, match TableStore columns
     *
     * @param clazz
     * @param <T>
     * @return
     */
    public <T> List<FieldInfo> getFields(Class<T> clazz) {
        List<FieldInfo> fieldInfos = fieldInfoCache.get(clazz);
        if (fieldInfos != null) {
            return fieldInfos;
        }
        synchronized (ClassInfo.class) {
            fieldInfos = fieldInfoCache.get(clazz);
            if (fieldInfos == null) {
                fieldInfos = new ArrayList<>();
                Class<?> currentClass = clazz;
                while (currentClass != null) {
                    loadFields(fieldInfos, currentClass);
                    currentClass = currentClass.getSuperclass();
                }
                fieldInfoCache.put(clazz, fieldInfos);
            }
        }
        return fieldInfos;
    }

    private void loadFields(List<FieldInfo> fieldInfos, Class<?> clazz) {
        Field[] declaredFields = clazz.getDeclaredFields();
        for (Field field : declaredFields) {
            int modifiers = field.getModifiers();
            if (Modifier.isFinal(modifiers) || Modifier.isStatic(modifiers)) {
                continue;
            }
            String columnName = field.getName();
            if (field.isAnnotationPresent(TableStoreColumn.class)) {
                TableStoreColumn column = field.getAnnotation(TableStoreColumn.class);
                if (column.ignore()) {
                    continue;
                }
                if (!"".equals(column.value())) {
                    columnName = column.value();
                }
                // Warm-up loading
                Class<? extends TableStoreConverter> converter = column.converter();
                converterCache.putIfAbsent(converter, newInstance(converter));
                Class<? extends TableStoreFormatter> formatter = column.formatter();
                formatterCache.putIfAbsent(formatter, newInstance(formatter));
                fieldInfos.add(new FieldInfo(field, columnName, converter, formatter, false,
                    false, null, TableStoreType.find(column.type())));
            } else if (field.isAnnotationPresent(TableStorePrimaryKey.class)) {
                TableStorePrimaryKey primaryKey = field.getAnnotation(TableStorePrimaryKey.class);
                if (primaryKey.ignore()) {
                    continue;
                }
                if (!"".equals(primaryKey.value())) {
                    columnName = primaryKey.value();
                }
                // Warm-up loading
                Class<? extends TableStoreConverter> converter = primaryKey.converter();
                converterCache.putIfAbsent(converter, newInstance(converter));
                Class<? extends TableStoreFormatter> formatter = primaryKey.formatter();
                formatterCache.putIfAbsent(formatter, newInstance(formatter));
                boolean autoIncrement = primaryKey.autoIncrement();
                PrimaryKeyType type;
                if (autoIncrement) {
                    type = PrimaryKeyType.INTEGER;
                } else {
                    type = primaryKey.type();
                }
                fieldInfos.add(new FieldInfo(field, columnName, converter, formatter, true,
                    autoIncrement, primaryKey.order(), TableStoreType.find(type)));
            } else {
                fieldInfos.add(
                    new FieldInfo(field, columnName, DefaultConverter.class, DefaultFormatter.class, false,
                        false, null, TableStoreType.find(derive(field.getType()))));
            }
            if (!field.isAccessible()) {
                field.setAccessible(true);
            }
        }
    }

    /**
     * derive field type to TableStore column type
     *
     * @param fieldType
     * @return
     */
    private ColumnType derive(Class<?> fieldType) {
        if (String.class.equals(fieldType)) {
            return ColumnType.STRING;
        }
        if (Long.class.equals(fieldType) || Long.TYPE.equals(fieldType)
            || Integer.class.equals(fieldType) || Integer.TYPE.equals(fieldType)) {
            return ColumnType.INTEGER;
        }
        if (Boolean.class.equals(fieldType) || Boolean.TYPE.equals(fieldType)) {
            return ColumnType.BOOLEAN;
        }
        if (Double.class.equals(fieldType) || Double.TYPE.equals(fieldType)
            || Float.class.equals(fieldType) || Float.TYPE.equals(fieldType)) {
            return ColumnType.DOUBLE;
        }
        if (Byte[].class.equals(fieldType) || byte[].class.equals(fieldType)) {
            return ColumnType.BINARY;
        }
        throw new IllegalArgumentException("cannot derive TableStore type from Java type");
    }

    private <T> T newInstance(Class<T> clazz) {
        try {
            return clazz.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
