package com.aliyun.iotx.fluentable.operation;

import java.util.ArrayList;
import java.util.List;

import com.alicloud.openservices.tablestore.model.MultiRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.RangeIteratorParameter;
import com.alicloud.openservices.tablestore.model.SingleRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.filter.Filter;

import static com.alicloud.openservices.tablestore.model.Direction.BACKWARD;
import static com.alicloud.openservices.tablestore.model.Direction.FORWARD;

/**
 * @author jiehong.jh
 * @date 2018/7/31
 */
public class TableStoreSelect {

    /**
     * 查询的表的名称。
     */
    private String tableName;
    /**
     * 要读取的属性列名列表，若为空，则代表读取该行所有的列。
     */
    private String[] columnNames;

    private Where where = new Where();

    /**
     * 读取行的所有列
     */
    public TableStoreSelect() {
    }

    /**
     * 读取行的指定列
     *
     * @param columnNames
     */
    public TableStoreSelect(String... columnNames) {
        this.columnNames = columnNames;
    }

    /**
     * 设置查询的表名
     *
     * @param tableName
     * @return
     */
    public TableStoreSelect from(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public Where where() {
        return where;
    }

    public class Where {

        /**
         * 单行读
         *
         * @param primaryKey
         * @return
         */
        public SingleRowQuery pkEqual(PrimaryKey primaryKey) {
            SingleRowQuery query = new SingleRowQuery();
            query.primaryKey = primaryKey;
            return query;
        }

        /**
         * 批量读
         *
         * @param primaryKeys
         * @return
         */
        public BatchRowQuery pkIn(List<PrimaryKey> primaryKeys) {
            BatchRowQuery query = new BatchRowQuery();
            query.primaryKeys.addAll(primaryKeys);
            return query;
        }

        /**
         * 范围读
         *
         * @param inclusiveStart
         * @param exclusiveEnd
         * @return
         */
        public RangeRowQuery pkBetween(PrimaryKey inclusiveStart, PrimaryKey exclusiveEnd) {
            RangeRowQuery query = new RangeRowQuery();
            query.inclusiveStart = inclusiveStart;
            query.exclusiveEnd = exclusiveEnd;
            return query;
        }
    }

    public class SingleRowQuery implements TableStoreRowQuery<SingleRowQuery> {

        private SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);

        private PrimaryKey primaryKey;

        private SingleRowQuery() {
            criteria.setMaxVersions(1);
        }

        @Override
        public SingleRowQuery maxVersions(int maxVersions) {
            criteria.setMaxVersions(maxVersions);
            return this;
        }

        @Override
        public SingleRowQuery filter(Filter filter) {
            criteria.setFilter(filter);
            return this;
        }

        @Override
        public SingleRowQueryCriteria rowQuery() {
            criteria.setPrimaryKey(primaryKey);
            if (columnNames != null) {
                criteria.addColumnsToGet(columnNames);
            }
            return criteria;
        }
    }

    public class BatchRowQuery implements TableStoreRowQuery<BatchRowQuery> {

        private MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);

        private List<PrimaryKey> primaryKeys = new ArrayList<>();

        private BatchRowQuery() {
            criteria.setMaxVersions(1);
        }

        @Override
        public BatchRowQuery maxVersions(int maxVersions) {
            criteria.setMaxVersions(maxVersions);
            return this;
        }

        @Override
        public BatchRowQuery filter(Filter filter) {
            criteria.setFilter(filter);
            return this;
        }

        @Override
        public MultiRowQueryCriteria rowQuery() {
            criteria.setRowKeys(primaryKeys);
            if (columnNames != null) {
                criteria.addColumnsToGet(columnNames);
            }
            return criteria;
        }
    }

    public class RangeRowQuery implements TableStoreRowQuery<RangeRowQuery> {

        private RangeIteratorParameter criteria = new RangeIteratorParameter(tableName);

        private PrimaryKey inclusiveStart;
        private PrimaryKey exclusiveEnd;

        private RangeRowQuery() {
            criteria.setMaxVersions(1);
            criteria.setBufferSize(100);
        }

        @Override
        public RangeRowQuery maxVersions(int maxVersions) {
            criteria.setMaxVersions(maxVersions);
            return this;
        }

        @Override
        public RangeRowQuery filter(Filter filter) {
            criteria.setFilter(filter);
            return this;
        }

        /**
         * 设置Iterator查询返回的最大行数，若count未设置，则返回查询范围下的所有行。默认-1代表不对行数做限制，读取该范围下所有行。
         *
         * @param maxCount 请求返回的行数。
         */
        public RangeRowQuery maxCount(int maxCount) {
            if (maxCount != -1) {
                criteria.setMaxCount(maxCount);
            }
            return this;
        }

        /**
         * 设置Buffer的大小，默认每次请求最多返回100行。
         * <p>
         * Iterator分批查询时用到的buffer的大小，该大小决定了Iterator调用GetRange查询时每次请求返回的最大行数。-1代表不设置buffer的大小，每次按TableStore一次请求最多返回行数来。
         *
         * @param bufferSize Buffer的大小。
         */
        public RangeRowQuery bufferSize(int bufferSize) {
            criteria.setBufferSize(bufferSize);
            return this;
        }

        /**
         * 排序
         *
         * @param forward true=正序，false=反序
         * @return
         */
        public RangeRowQuery orderBy(boolean forward) {
            criteria.setDirection(forward ? FORWARD : BACKWARD);
            return this;
        }

        @Override
        public RangeIteratorParameter rowQuery() {
            criteria.setInclusiveStartPrimaryKey(inclusiveStart);
            criteria.setExclusiveEndPrimaryKey(exclusiveEnd);
            if (columnNames != null) {
                criteria.addColumnsToGet(columnNames);
            }
            return criteria;
        }
    }
}
