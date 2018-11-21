package com.aliyun.iotx.fluentable.model;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.alicloud.openservices.tablestore.model.PrimaryKey;

/**
 * @author jiehong.jh
 * @date 2018/9/7
 */
public class OptionalRow {

    private static final OptionalRow EMPTY = new OptionalRow();

    private PrimaryKey primaryKey;

    private List<OptionalColumn> columns;

    private OptionalRow() {
    }

    public OptionalRow(int capacity) {
        this.columns = new ArrayList<>(capacity);
    }

    public static OptionalRow empty() {
        return EMPTY;
    }

    public void primaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
    }

    public PrimaryKey primaryKey() {
        return primaryKey;
    }

    /**
     * @param name column name
     * @return
     */
    public List<OptionalColumn> getColumns(String name) {
        if (columns == null || name == null) {
            return null;
        }
        return columns.stream()
            .filter(column -> name.equals(column.getName()))
            .collect(Collectors.toList());
    }

    public List<OptionalColumn> getColumns() {
        return columns;
    }

    public OptionalRow put(OptionalColumn column) {
        if (column == null || column.getName() == null) {
            return this;
        }
        if (columns != null) {
            columns.add(column);
        }
        return this;
    }
}
