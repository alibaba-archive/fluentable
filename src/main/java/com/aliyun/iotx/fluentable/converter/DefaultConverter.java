package com.aliyun.iotx.fluentable.converter;

import java.math.BigDecimal;
import java.util.Date;

import com.aliyun.iotx.fluentable.parser.TableStoreType;

/**
 * @author jiehong.jh
 * @date 2018/8/2
 */
public class DefaultConverter implements TableStoreConverter {

    @Override
    public Object convert(TableStoreType columnEnum, Object columnValue, Class<?> fieldType) {
        switch (columnEnum) {
            case STRING:
                return convert((String)columnValue, fieldType);
            case BOOLEAN:
                return convert((Boolean)columnValue, fieldType);
            case INTEGER:
                return convert((Long)columnValue, fieldType);
            case DOUBLE:
                return convert((Double)columnValue, fieldType);
            case BINARY:
                return convert((byte[])columnValue, fieldType);
            default:
                throw new UnsupportedOperationException(columnEnum.name() + " type not support.");
        }
    }

    protected Object convert(String columnValue, Class<?> fieldType) {
        if (String.class.equals(fieldType)) {
            return columnValue;
        }
        if (Integer.class.equals(fieldType) || Integer.TYPE.equals(fieldType)) {
            return Integer.valueOf(columnValue);
        }
        if (Long.class.equals(fieldType) || Long.TYPE.equals(fieldType)) {
            return Long.valueOf(columnValue);
        }
        if (BigDecimal.class.equals(fieldType)) {
            return new BigDecimal(columnValue);
        }
        throw new IllegalArgumentException("String cannot convert to " + fieldType.getSimpleName());
    }

    protected Object convert(Boolean columnValue, Class<?> fieldType) {
        if (Boolean.class.equals(fieldType) || Boolean.TYPE.equals(fieldType)) {
            return columnValue;
        }
        if (String.class.equals(fieldType)) {
            return columnValue.toString();
        }
        throw new IllegalArgumentException("Boolean cannot convert to " + fieldType.getSimpleName());
    }

    protected Object convert(Long columnValue, Class<?> fieldType) {
        if (Long.class.equals(fieldType) || Long.TYPE.equals(fieldType)) {
            return columnValue;
        }
        if (Integer.class.equals(fieldType) || Integer.TYPE.equals(fieldType)) {
            return (int)((long)columnValue);
        }
        if (String.class.equals(fieldType)) {
            return columnValue.toString();
        }
        if (BigDecimal.class.equals(fieldType)) {
            return new BigDecimal(columnValue);
        }
        if (Date.class.equals(fieldType)) {
            return new Date(columnValue);
        }
        throw new IllegalArgumentException("Long cannot convert to " + fieldType.getSimpleName());
    }

    protected Object convert(Double columnValue, Class<?> fieldType) {
        if (Double.class.equals(fieldType) || Double.TYPE.equals(fieldType)) {
            return columnValue;
        }
        if (Float.class.equals(fieldType) || Float.TYPE.equals(fieldType)) {
            return (float)((double)columnValue);
        }
        if (String.class.equals(fieldType)) {
            return columnValue.toString();
        }
        if (BigDecimal.class.equals(fieldType)) {
            return BigDecimal.valueOf(columnValue);
        }
        throw new IllegalArgumentException("Double cannot convert to " + fieldType.getSimpleName());
    }

    protected Object convert(byte[] columnValue, Class<?> fieldType) {
        if (byte[].class.equals(fieldType)) {
            return columnValue;
        }
        if (Byte[].class.equals(fieldType)) {
            Byte[] byteObjects = new Byte[columnValue.length];
            for (int i = 0; i < columnValue.length; i++) {
                byteObjects[i] = columnValue[i];
            }
            return byteObjects;
        }
        if (String.class.equals(fieldType)) {
            return new String(columnValue);
        }
        throw new IllegalArgumentException("byte[] cannot convert to " + fieldType.getSimpleName());
    }
}
