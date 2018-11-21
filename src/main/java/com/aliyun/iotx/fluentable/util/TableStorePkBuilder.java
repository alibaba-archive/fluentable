package com.aliyun.iotx.fluentable.util;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;

/**
 * 构建TableStore PrimaryKey
 * <p>
 * 注意：主键列的顺序与创建表时TableMeta中定义的一致
 *
 * @author jiehong.jh
 * @date 2018/6/1
 */
public class TableStorePkBuilder {

    private PrimaryKeyBuilder builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();

    public TableStorePkBuilder add(PrimaryKeyColumn column) {
        builder.addPrimaryKeyColumn(column);
        return this;
    }

    public TableStorePkBuilder add(String name, String value) {
        builder.addPrimaryKeyColumn(name, PrimaryKeyValue.fromString(value));
        return this;
    }

    public TableStorePkBuilder add(String name, Integer value) {
        builder.addPrimaryKeyColumn(name, PrimaryKeyValue.fromLong(value));
        return this;
    }

    public TableStorePkBuilder add(String name, Long value) {
        builder.addPrimaryKeyColumn(name, PrimaryKeyValue.fromLong(value));
        return this;
    }

    public TableStorePkBuilder add(String name, byte[] value) {
        builder.addPrimaryKeyColumn(name, PrimaryKeyValue.fromBinary(value));
        return this;
    }

    public TableStorePkBuilder add(String name, ColumnValue value) {
        builder.addPrimaryKeyColumn(name, PrimaryKeyValue.fromColumn(value));
        return this;
    }

    public TableStorePkBuilder addMin(String name) {
        builder.addPrimaryKeyColumn(name, PrimaryKeyValue.INF_MIN);
        return this;
    }

    public TableStorePkBuilder addMax(String name) {
        builder.addPrimaryKeyColumn(name, PrimaryKeyValue.INF_MAX);
        return this;
    }

    public TableStorePkBuilder addAuto(String name) {
        builder.addPrimaryKeyColumn(name, PrimaryKeyValue.AUTO_INCREMENT);
        return this;
    }

    public PrimaryKey build() {
        return builder.build();
    }

}
