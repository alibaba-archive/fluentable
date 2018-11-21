package com.aliyun.iotx.fluentable.formatter;

import java.math.BigDecimal;
import java.util.Date;

import com.aliyun.iotx.fluentable.model.OptionalColumnValue;
import com.aliyun.iotx.fluentable.parser.TableStoreType;

/**
 * @author jiehong.jh
 * @date 2018/9/7
 */
public class DefaultFormatter implements TableStoreFormatter {

    @Override
    public OptionalColumnValue format(TableStoreType columnEnum, Object fieldValue, Class<?> fieldType) {
        if (fieldValue == null) {
            return OptionalColumnValue.empty();
        }
        switch (columnEnum) {
            case STRING:
                return OptionalColumnValue.of(fieldValue.toString());
            case BOOLEAN:
                return OptionalColumnValue.of(convertBoolean(fieldValue, fieldType));
            case INTEGER:
                return OptionalColumnValue.of(convertLong(fieldValue, fieldType));
            case DOUBLE:
                return OptionalColumnValue.of(convertDouble(fieldValue, fieldType));
            case BINARY:
                return OptionalColumnValue.of(convertByteArray(fieldValue, fieldType));
            default:
                throw new UnsupportedOperationException(columnEnum.name() + " type not support.");
        }
    }

    protected Boolean convertBoolean(Object fieldValue, Class<?> fieldType) {
        if (Boolean.class.equals(fieldType) || Boolean.TYPE.equals(fieldType)) {
            return (boolean)fieldValue;
        }
        if (String.class.equals(fieldType)) {
            return Boolean.valueOf((String)fieldValue);
        }
        throw new IllegalArgumentException("Boolean cannot convert from " + fieldType.getSimpleName());
    }

    protected Long convertLong(Object fieldValue, Class<?> fieldType) {
        if (Long.class.equals(fieldType) || Long.TYPE.equals(fieldType)) {
            return (long)fieldValue;
        }
        if (Integer.class.equals(fieldType) || Integer.TYPE.equals(fieldType)) {
            return (long)(int)fieldValue;
        }
        if (String.class.equals(fieldType)) {
            return Long.valueOf((String)fieldValue);
        }
        if (Date.class.equals(fieldType)) {
            return ((Date)fieldValue).getTime();
        }
        throw new IllegalArgumentException("Long cannot convert from " + fieldType.getSimpleName());
    }

    protected Double convertDouble(Object fieldValue, Class<?> fieldType) {
        if (Double.class.equals(fieldType) || Double.TYPE.equals(fieldType)) {
            return (double)fieldValue;
        }
        if (Float.class.equals(fieldType) || Float.TYPE.equals(fieldType)) {
            return (double)(float)fieldValue;
        }
        if (String.class.equals(fieldType)) {
            return Double.valueOf((String)fieldValue);
        }
        if (BigDecimal.class.equals(fieldType)) {
            return ((BigDecimal)fieldValue).doubleValue();
        }
        throw new IllegalArgumentException("Double cannot convert from " + fieldType.getSimpleName());
    }

    protected byte[] convertByteArray(Object fieldValue, Class<?> fieldType) {
        if (byte[].class.equals(fieldType)) {
            return (byte[])fieldValue;
        }
        if (Byte[].class.equals(fieldType)) {
            Byte[] byteObjects = (Byte[])fieldValue;
            byte[] bytes = new byte[byteObjects.length];
            for (int i = 0; i < byteObjects.length; i++) {
                bytes[i] = byteObjects[i];
            }
            return bytes;
        }
        if (String.class.equals(fieldType)) {
            return ((String)fieldValue).getBytes();
        }
        throw new IllegalArgumentException("byte[] cannot convert from " + fieldType.getSimpleName());
    }
}
