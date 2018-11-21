package com.aliyun.iotx.fluentable.parser;

import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;

/**
 * @author jiehong.jh
 * @date 2018/8/2
 */
public enum TableStoreType {

    /**
     * 字符串型。
     */
    STRING(ColumnType.STRING, PrimaryKeyType.STRING),

    /**
     * 64位带符号的整型。
     */
    INTEGER(ColumnType.INTEGER, PrimaryKeyType.INTEGER),

    /**
     * 布尔型。
     */
    BOOLEAN(ColumnType.BOOLEAN, null),

    /**
     * 64位浮点型。
     */
    DOUBLE(ColumnType.DOUBLE, null),

    /**
     * 二进制数据。
     */
    BINARY(ColumnType.BINARY, PrimaryKeyType.BINARY);

    private ColumnType columnType;
    private PrimaryKeyType primaryKeyType;

    TableStoreType(ColumnType columnType, PrimaryKeyType primaryKeyType) {
        this.columnType = columnType;
        this.primaryKeyType = primaryKeyType;
    }

    public static TableStoreType find(ColumnType columnType) {
        for (TableStoreType columnEnum : values()) {
            if (columnEnum.columnType == columnType) {
                return columnEnum;
            }
        }
        throw new UnsupportedOperationException(columnType.name() + " type not support.");
    }

    public static TableStoreType find(PrimaryKeyType primaryKeyType) {
        for (TableStoreType columnEnum : values()) {
            if (columnEnum.primaryKeyType == null) {
                continue;
            }
            if (columnEnum.primaryKeyType == primaryKeyType) {
                return columnEnum;
            }
        }
        throw new UnsupportedOperationException(primaryKeyType.name() + " type not support.");
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public PrimaryKeyType getPrimaryKeyType() {
        return primaryKeyType;
    }
}
