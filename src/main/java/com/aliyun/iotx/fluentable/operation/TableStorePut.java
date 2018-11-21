package com.aliyun.iotx.fluentable.operation;

import java.util.List;

import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.Condition;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.RowPutChange;
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
public class TableStorePut {

    private RowPutChange change;
    /**
     * Judge condition
     */
    private Condition condition;

    private Where where = new Where();
    private RowPut put = new RowPut();

    public TableStorePut(String tableName) {
        change = new RowPutChange(tableName);
    }

    /**
     * add a column
     *
     * @param name:  column name
     * @param value: column value
     * @return this: (for invocation chain)
     */
    public TableStorePut add(String name, ColumnValue value) {
        change.addColumn(name, value);
        return this;
    }

    /**
     * add a column
     *
     * @param name:  column name
     * @param value: column value
     * @return this: (for invocation chain)
     */
    public TableStorePut add(String name, OptionalColumnValue value) {
        if (value.isPresent()) {
            change.addColumn(name, value.get());
        }
        return this;
    }

    /**
     * add a column with timestamp
     *
     * @param name:  column name
     * @param value: column value
     * @param ts:    column timestamp
     * @return this (for invocation chain)
     */
    public TableStorePut add(String name, ColumnValue value, long ts) {
        change.addColumn(name, value, ts);
        return this;
    }

    /**
     * add a column with timestamp
     *
     * @param name:  column name
     * @param value: column value
     * @param ts:    column timestamp
     * @return this (for invocation chain)
     */
    public TableStorePut add(String name, OptionalColumnValue value, long ts) {
        if (value.isPresent()) {
            change.addColumn(name, value.get(), ts);
        }
        return this;
    }

    /**
     * add a list of columns <p>write column orders coordinate with list order</p>
     *
     * @param columns: column list
     * @return this: (for invocation chain)
     */
    public TableStorePut add(List<Column> columns) {
        change.addColumns(columns);
        return this;
    }

    /**
     * add a list of columns <p>write column orders coordinate with array order</p>
     *
     * @param columns: column array
     * @return this: (for invocation chain)
     */
    public TableStorePut add(Column... columns) {
        change.addColumns(columns);
        return this;
    }

    /**
     * 添加 Row
     *
     * @param row
     * @return
     */
    public Where with(OptionalRow row) {
        List<OptionalColumn> columns = row.getColumns();
        if (columns != null && !columns.isEmpty()) {
            columns.forEach(column -> {
                Long ts = column.getTs();
                if (ts == null) {
                    add(column.getName(), column.getValue());
                } else {
                    add(column.getName(), column.getValue(), ts);
                }
            });
        }
        return where(row.primaryKey());
    }

    public Where where(PrimaryKey primaryKey) {
        change.setPrimaryKey(primaryKey);
        return where;
    }

    public class Where {

        /**
         * Expect the row exist
         *
         * @return
         */
        public RowPut rowExist() {
            condition = new Condition(EXPECT_EXIST);
            return put;
        }

        /**
         * Expect the row not exist
         *
         * @return
         */
        public RowPut rowNotExist() {
            condition = new Condition(EXPECT_NOT_EXIST);
            return put;
        }

        /**
         * Ignore where the row exists or not
         *
         * @return
         */
        public RowPut rowIgnore() {
            condition = new Condition(IGNORE);
            return put;
        }
    }

    public class RowPut implements TableStoreRowChange<RowPut> {

        @Override
        public RowPut condition(ColumnCondition columnCondition) {
            condition.setColumnCondition(columnCondition);
            return this;
        }

        @Override
        public RowPutChange rowChange() {
            change.setCondition(condition);
            return change;
        }
    }
}
