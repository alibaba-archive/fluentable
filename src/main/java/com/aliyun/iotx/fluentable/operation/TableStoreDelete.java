package com.aliyun.iotx.fluentable.operation;

import com.alicloud.openservices.tablestore.model.Condition;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.RowDeleteChange;
import com.alicloud.openservices.tablestore.model.condition.ColumnCondition;

import static com.alicloud.openservices.tablestore.model.RowExistenceExpectation.EXPECT_EXIST;
import static com.alicloud.openservices.tablestore.model.RowExistenceExpectation.EXPECT_NOT_EXIST;
import static com.alicloud.openservices.tablestore.model.RowExistenceExpectation.IGNORE;

/**
 * @author jiehong.jh
 * @date 2018/7/31
 */
public class TableStoreDelete {

    private RowDeleteChange change;
    /**
     * Judge condition
     */
    private Condition condition;

    private Where where = new Where();
    private RowDelete delete = new RowDelete();

    public TableStoreDelete(String tableName) {
        change = new RowDeleteChange(tableName);
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
        public RowDelete rowExist() {
            condition = new Condition(EXPECT_EXIST);
            return delete;
        }

        /**
         * Expect the row not exist
         *
         * @return
         */
        public RowDelete rowNotExist() {
            condition = new Condition(EXPECT_NOT_EXIST);
            return delete;
        }

        /**
         * Ignore where the row exists or not
         *
         * @return
         */
        public RowDelete rowIgnore() {
            condition = new Condition(IGNORE);
            return delete;
        }
    }

    public class RowDelete implements TableStoreRowChange<RowDelete> {

        @Override
        public RowDelete condition(ColumnCondition columnCondition) {
            condition.setColumnCondition(columnCondition);
            return this;
        }

        @Override
        public RowDeleteChange rowChange() {
            change.setCondition(condition);
            return change;
        }
    }
}
