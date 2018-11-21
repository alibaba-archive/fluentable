package com.aliyun.iotx.fluentable.util;

import java.util.Arrays;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.condition.ColumnCondition;
import com.alicloud.openservices.tablestore.model.condition.CompositeColumnValueCondition;
import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;

import static com.alicloud.openservices.tablestore.model.condition.CompositeColumnValueCondition.LogicOperator.AND;
import static com.alicloud.openservices.tablestore.model.condition.CompositeColumnValueCondition.LogicOperator.NOT;
import static com.alicloud.openservices.tablestore.model.condition.CompositeColumnValueCondition.LogicOperator.OR;
import static com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition.CompareOperator.EQUAL;
import static com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition.CompareOperator
    .GREATER_EQUAL;
import static com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition.CompareOperator
    .GREATER_THAN;
import static com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition.CompareOperator
    .LESS_EQUAL;
import static com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition.CompareOperator.LESS_THAN;
import static com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition.CompareOperator.NOT_EQUAL;

/**
 * Construct TableStore ColumnCondition
 * <p>
 * 则若列在该行中不存在，则条件通过；只会对最新版本的值进行比较
 *
 * @author jiehong.jh
 * @date 2018/6/1
 */
public class TableStoreConditions {

    public static SingleColumnValueCondition eq(String name, ColumnValue value) {
        return new SingleColumnValueCondition(name, EQUAL, value);
    }

    public static SingleColumnValueCondition ne(String name, ColumnValue value) {
        return new SingleColumnValueCondition(name, NOT_EQUAL, value);
    }

    public static SingleColumnValueCondition gt(String name, ColumnValue value) {
        return new SingleColumnValueCondition(name, GREATER_THAN, value);
    }

    public static SingleColumnValueCondition ge(String name, ColumnValue value) {
        return new SingleColumnValueCondition(name, GREATER_EQUAL, value);
    }

    public static SingleColumnValueCondition lt(String name, ColumnValue value) {
        return new SingleColumnValueCondition(name, LESS_THAN, value);
    }

    public static SingleColumnValueCondition le(String name, ColumnValue value) {
        return new SingleColumnValueCondition(name, LESS_EQUAL, value);
    }

    public static CompositeColumnValueCondition not(ColumnCondition condition) {
        CompositeColumnValueCondition compositeCondition = new CompositeColumnValueCondition(NOT);
        compositeCondition.addCondition(condition);
        return compositeCondition;
    }

    public static CompositeColumnValueCondition and(ColumnCondition condition, ColumnCondition... conditions) {
        CompositeColumnValueCondition compositeCondition = new CompositeColumnValueCondition(AND);
        compositeCondition.addCondition(condition);
        Arrays.stream(conditions).forEach(compositeCondition::addCondition);
        return compositeCondition;
    }

    public static CompositeColumnValueCondition or(ColumnCondition condition, ColumnCondition... conditions) {
        CompositeColumnValueCondition compositeCondition = new CompositeColumnValueCondition(OR);
        compositeCondition.addCondition(condition);
        Arrays.stream(conditions).forEach(compositeCondition::addCondition);
        return compositeCondition;
    }

}
