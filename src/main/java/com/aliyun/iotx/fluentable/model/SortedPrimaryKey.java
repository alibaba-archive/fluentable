package com.aliyun.iotx.fluentable.model;

import com.alicloud.openservices.tablestore.model.ColumnValue;

/**
 * @author jiehong.jh
 * @date 2018/9/7
 */
public class SortedPrimaryKey implements Comparable<SortedPrimaryKey> {

    /**
     * primary key order
     */
    private int order;
    /**
     * column name
     */
    private String name;
    private boolean autoIncrement;
    private ColumnValue value;

    public SortedPrimaryKey(int order, String name, boolean autoIncrement, ColumnValue value) {
        this.order = order;
        this.name = name;
        this.autoIncrement = autoIncrement;
        this.value = value;
    }

    public int getOrder() {
        return order;
    }

    public String getName() {
        return name;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public ColumnValue getValue() {
        return value;
    }

    @Override
    public int compareTo(SortedPrimaryKey o) {
        return order - o.order;
    }
}
