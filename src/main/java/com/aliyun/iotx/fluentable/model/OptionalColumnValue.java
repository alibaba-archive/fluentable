package com.aliyun.iotx.fluentable.model;

import java.util.NoSuchElementException;
import java.util.Objects;

import com.alicloud.openservices.tablestore.model.ColumnValue;

/**
 * @author jiehong.jh
 * @date 2018/8/28
 */
public class OptionalColumnValue {

    private static final OptionalColumnValue EMPTY = new OptionalColumnValue();

    private final ColumnValue value;

    private OptionalColumnValue() {
        this.value = null;
    }

    private OptionalColumnValue(ColumnValue value) {
        this.value = Objects.requireNonNull(value);
    }

    public static OptionalColumnValue empty() {
        return EMPTY;
    }

    public static OptionalColumnValue of(String value) {
        if (value == null) {
            return EMPTY;
        }
        return new OptionalColumnValue(ColumnValue.fromString(value));
    }

    public static OptionalColumnValue of(Integer value) {
        if (value == null) {
            return EMPTY;
        }
        return new OptionalColumnValue(ColumnValue.fromLong(value));
    }

    public static OptionalColumnValue of(Long value) {
        if (value == null) {
            return EMPTY;
        }
        return new OptionalColumnValue(ColumnValue.fromLong(value));
    }

    public static OptionalColumnValue of(byte[] value) {
        if (value == null) {
            return EMPTY;
        }
        return new OptionalColumnValue(ColumnValue.fromBinary(value));
    }

    public static OptionalColumnValue of(Float value) {
        if (value == null) {
            return EMPTY;
        }
        return new OptionalColumnValue(ColumnValue.fromDouble(value));
    }

    public static OptionalColumnValue of(Double value) {
        if (value == null) {
            return EMPTY;
        }
        return new OptionalColumnValue(ColumnValue.fromDouble(value));
    }

    public static OptionalColumnValue of(Boolean value) {
        if (value == null) {
            return EMPTY;
        }
        return new OptionalColumnValue(ColumnValue.fromBoolean(value));
    }

    public boolean isPresent() {
        return value != null;
    }

    public ColumnValue get() {
        if (value == null) {
            throw new NoSuchElementException("No value present");
        }
        return value;
    }
}
