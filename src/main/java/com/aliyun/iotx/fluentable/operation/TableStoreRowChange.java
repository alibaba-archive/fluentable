package com.aliyun.iotx.fluentable.operation;

import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.model.condition.ColumnCondition;

/**
 * @author jiehong.jh
 * @date 2018/7/31
 */
public interface TableStoreRowChange<T extends TableStoreRowChange<T>> {

    /**
     * Set Judge condition with column condition
     *
     * @param columnCondition
     * @return
     */
    T condition(ColumnCondition columnCondition);

    /**
     * row change
     *
     * @return
     */
    RowChange rowChange();
}
