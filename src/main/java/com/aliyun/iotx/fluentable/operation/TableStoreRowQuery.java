package com.aliyun.iotx.fluentable.operation;

import com.alicloud.openservices.tablestore.model.RowQueryCriteria;
import com.alicloud.openservices.tablestore.model.filter.Filter;

/**
 * @author jiehong.jh
 * @date 2018/7/31
 */
public interface TableStoreRowQuery<T extends TableStoreRowQuery<T>> {

    /**
     * Set the max number of versions for the return
     *
     * @param maxVersions
     * @return
     */
    T maxVersions(int maxVersions);

    /**
     * Set the filter for the query
     *
     * @param filter
     * @return
     */
    T filter(Filter filter);

    /**
     * row query criteria
     *
     * @return
     */
    RowQueryCriteria rowQuery();
}
