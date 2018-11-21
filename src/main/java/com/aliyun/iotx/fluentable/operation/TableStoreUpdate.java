package com.aliyun.iotx.fluentable.operation;

import java.util.List;

import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.Condition;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.alicloud.openservices.tablestore.model.condition.ColumnCondition;
import com.aliyun.iotx.fluentable.model.OptionalColumn;
import com.aliyun.iotx.fluentable.model.OptionalColumnValue;
import com.aliyun.iotx.fluentable.model.OptionalRow;

import static com.alicloud.openservices.tablestore.model.RowExistenceExpectation.EXPECT_EXIST;
import static com.alicloud.openservices.tablestore.model.RowExistenceExpectation.EXPECT_NOT_EXIST;
import static com.alicloud.openservices.tablestore.model.RowExistenceExpectation.IGNORE;

/**
 * @author jiehong.jh
 * @date 2018/7/31
 */
public class TableStoreUpdate {

    private RowUpdateChange change;
    /**
     * Judge condition
     */
    private Condition condition;

    private Where where = new Where();
    private RowUpate update = new RowUpate();

    public TableStoreUpdate(String tableName) {
        change = new RowUpdateChange(tableName);
    }

    /**
     * 新写入一个属性列。
     *
     * @param column
     * @return this (for invocation chain)
     */
    public TableStoreUpdate put(Column column) {
        change.put(column);
        return this;
    }

    /**
     * 新写入一个属性列。
     *
     * @param name  属性列的名称
     * @param value 属性列的值
     * @return this (for invocation chain)
     */
    public TableStoreUpdate put(String name, ColumnValue value) {
        change.put(name, value);
        return this;
    }

    /**
     * 新写入一个属性列。
     *
     * @param name  属性列的名称
     * @param value 属性列的值
     * @param ts    属性列的时间戳
     * @return this (for invocation chain)
     */
    public TableStoreUpdate put(String name, ColumnValue value, long ts) {
        change.put(name, value, ts);
        return this;
    }

    /**
     * 新写入一个属性列，有数据写入。
     *
     * @param name  属性列的名称
     * @param value 属性列的值
     * @return this (for invocation chain)
     */
    public TableStoreUpdate put(String name, OptionalColumnValue value) {
        if (value.isPresent()) {
            change.put(name, value.get());
        }
        return this;
    }

    /**
     * 新写入一个属性列，有数据写入。
     *
     * @param name  属性列的名称
     * @param value 属性列的值
     * @param ts    属性列的时间戳
     * @return this (for invocation chain)
     */
    public TableStoreUpdate put(String name, OptionalColumnValue value, long ts) {
        if (value.isPresent()) {
            change.put(name, value.get(), ts);
        }
        return this;
    }

    /**
     * 新写入一批属性列。 <p>属性列的写入顺序与列表中的顺序一致。</p>
     *
     * @param columns 属性列列表
     * @return this (for invocation chain)
     */
    public TableStoreUpdate put(List<Column> columns) {
        change.put(columns);
        return this;
    }

    /**
     * 删除某一属性列的特定版本。
     *
     * @param name 属性列的名称
     * @param ts   属性列的时间戳
     * @return this for chain invocation
     */
    public TableStoreUpdate delete(String name, long ts) {
        change.deleteColumn(name, ts);
        return this;
    }

    /**
     * 删除某一属性列的所有版本。
     *
     * @param name 属性列的名称
     * @return this for chain invocation
     */
    public TableStoreUpdate delete(String name) {
        change.deleteColumns(name);
        return this;
    }

    /**
     * 更新一个属性列，有数据写入，无数据则删除。
     *
     * @param name  属性列的名称
     * @param value 属性列的值
     * @return this (for invocation chain)
     */
    public TableStoreUpdate set(String name, OptionalColumnValue value) {
        if (value.isPresent()) {
            put(name, value.get());
        } else {
            delete(name);
        }
        return this;
    }

    /**
     * 更新一个属性列，有数据写入，无数据则删除。
     *
     * @param name  属性列的名称
     * @param value 属性列的值
     * @param ts    属性列的时间戳
     * @return this (for invocation chain)
     */
    public TableStoreUpdate set(String name, OptionalColumnValue value, long ts) {
        if (value.isPresent()) {
            put(name, value.get(), ts);
        } else {
            delete(name, ts);
        }
        return this;
    }

    /**
     * 增量变更
     *
     * @param name  属性列的名称
     * @param value 增量变更值
     * @return
     */
    public TableStoreUpdate inc(String name, long value) {
        change.increment(new Column(name, ColumnValue.fromLong(value)));
        return this;
    }

    /**
     * 更新 Row，Java属性为null将删除TableStore的字段
     *
     * @param row
     * @return
     */
    public Where with(OptionalRow row) {
        return with(row, false);
    }

    /**
     * 更新 Row
     *
     * @param row
     * @param ignoreNull Java属性为null，true=将忽略更新TableStore的字段，false=将删除TableStore的字段
     * @return
     */
    public Where with(OptionalRow row, boolean ignoreNull) {
        List<OptionalColumn> columns = row.getColumns();
        if (columns != null && !columns.isEmpty()) {
            if (ignoreNull) {
                columns.forEach(column -> {
                    OptionalColumnValue value = column.getValue();
                    if (value.isPresent()) {
                        Long ts = column.getTs();
                        if (ts == null) {
                            put(column.getName(), value.get());
                        } else {
                            put(column.getName(), value.get(), ts);
                        }
                    }
                });
            } else {
                columns.forEach(column -> {
                    Long ts = column.getTs();
                    if (ts == null) {
                        set(column.getName(), column.getValue());
                    } else {
                        set(column.getName(), column.getValue(), ts);
                    }
                });
            }
        }
        return where(row.primaryKey());
    }

    public Where where(PrimaryKey primaryKey) {
        change.setPrimaryKey(primaryKey);
        return where;
    }

    public class Where {

        /**
         * 期望该行存在。
         *
         * @return
         */
        public RowUpate rowExist() {
            condition = new Condition(EXPECT_EXIST);
            return update;
        }

        /**
         * 期望该行不存在
         *
         * @return
         */
        public RowUpate rowNotExist() {
            condition = new Condition(EXPECT_NOT_EXIST);
            return update;
        }

        /**
         * 不对行是否存在做任何判断。
         *
         * @return
         */
        public RowUpate rowIgnore() {
            condition = new Condition(IGNORE);
            return update;
        }
    }

    public class RowUpate implements TableStoreRowChange<RowUpate> {

        @Override
        public RowUpate condition(ColumnCondition columnCondition) {
            condition.setColumnCondition(columnCondition);
            return this;
        }

        @Override
        public RowUpdateChange rowChange() {
            change.setCondition(condition);
            return change;
        }
    }
}
