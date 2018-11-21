package com.aliyun.iotx.fluentable.util;

import java.util.Arrays;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.filter.ColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;

import static com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter.LogicOperator.AND;
import static com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter.LogicOperator.NOT;
import static com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter.LogicOperator.OR;
import static com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter.CompareOperator.EQUAL;
import static com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter.CompareOperator.GREATER_EQUAL;
import static com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter.CompareOperator.GREATER_THAN;
import static com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter.CompareOperator.LESS_EQUAL;
import static com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter.CompareOperator.LESS_THAN;
import static com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter.CompareOperator.NOT_EQUAL;

/**
 * 构建TableStore Filter
 * <p>
 * 若列在该行中不存在，则不返回该行；只会对最新版本的值进行比较
 *
 * @author jiehong.jh
 * @date 2018/6/1
 */
public class TableStoreFilters {

    public static SingleColumnValueFilter eq(String name, ColumnValue value) {
        return new SingleColumnValueFilter(name, EQUAL, value).setPassIfMissing(false);
    }

    public static SingleColumnValueFilter ne(String name, ColumnValue value) {
        return new SingleColumnValueFilter(name, NOT_EQUAL, value).setPassIfMissing(false);
    }

    public static SingleColumnValueFilter gt(String name, ColumnValue value) {
        return new SingleColumnValueFilter(name, GREATER_THAN, value).setPassIfMissing(false);
    }

    public static SingleColumnValueFilter ge(String name, ColumnValue value) {
        return new SingleColumnValueFilter(name, GREATER_EQUAL, value).setPassIfMissing(false);
    }

    public static SingleColumnValueFilter lt(String name, ColumnValue value) {
        return new SingleColumnValueFilter(name, LESS_THAN, value).setPassIfMissing(false);
    }

    public static SingleColumnValueFilter le(String name, ColumnValue value) {
        return new SingleColumnValueFilter(name, LESS_EQUAL, value).setPassIfMissing(false);
    }

    public static CompositeColumnValueFilter not(ColumnValueFilter filter) {
        CompositeColumnValueFilter compositeFilter = new CompositeColumnValueFilter(NOT);
        compositeFilter.addFilter(filter);
        return compositeFilter;
    }

    public static CompositeColumnValueFilter and(ColumnValueFilter filter, ColumnValueFilter... filters) {
        CompositeColumnValueFilter compositeFilter = new CompositeColumnValueFilter(AND);
        compositeFilter.addFilter(filter);
        Arrays.stream(filters).forEach(compositeFilter::addFilter);
        return compositeFilter;
    }

    public static CompositeColumnValueFilter or(ColumnValueFilter filter, ColumnValueFilter... filters) {
        CompositeColumnValueFilter compositeFilter = new CompositeColumnValueFilter(OR);
        compositeFilter.addFilter(filter);
        Arrays.stream(filters).forEach(compositeFilter::addFilter);
        return compositeFilter;
    }

}
