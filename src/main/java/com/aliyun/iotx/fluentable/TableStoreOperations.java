package com.aliyun.iotx.fluentable;

import com.aliyun.iotx.fluentable.operation.TableStoreDelete;
import com.aliyun.iotx.fluentable.operation.TableStorePut;
import com.aliyun.iotx.fluentable.operation.TableStoreSelect;
import com.aliyun.iotx.fluentable.operation.TableStoreUpdate;

/**
 * TableStore operations <p>In most cases, the following idiom should be used: <h3>Single Select</h3>
 * <pre> {@code
 * // 构造主键
 * PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
 * primaryKeyBuilder.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(pkValue));
 * PrimaryKey primaryKey = primaryKeyBuilder.build();
 * // 读一行
 * SingleRowQueryCriteria criteria = new SingleRowQueryCriteria("table", primaryKey);
 * // 设置读取最新版本
 * criteria.setMaxVersions(1);
 * // 设置过滤器, 当Col0的值为0时返回该行.
 * SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("Col0",
 * SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromLong(0));
 * // 如果不存在Col0这一列, 也不返回.
 * singleColumnValueFilter.setPassIfMissing(false);
 * criteria.setFilter(singleColumnValueFilter);
 * GetRowRequest request = new GetRowRequest(criteria);
 *
 * equal to:
 *
 * tableStoreTemplate.select().from("table").where()
 * .pkEqual(new TableStorePkBuilder().add("pk", pkValue).build())
 * .filter(eq("Col0", ColumnValue.fromLong(0))).rowQuery();
 * }</pre>
 * <p>
 * <h3>Range Select</h3>
 * <pre> {@code
 * RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria("table");
 * // 设置起始主键
 * PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
 * primaryKeyBuilder.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(startPkValue));
 * rangeRowQueryCriteria.setInclusiveStartPrimaryKey(primaryKeyBuilder.build());
 * // 设置结束主键
 * primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
 * primaryKeyBuilder.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(endPkValue));
 * rangeRowQueryCriteria.setExclusiveEndPrimaryKey(primaryKeyBuilder.build());
 * rangeRowQueryCriteria.setMaxVersions(1);
 * GetRangeRequest request = new GetRangeRequest(rangeRowQueryCriteria);
 *
 * equal to:
 *
 * tableStoreTemplate.select().from("table").where()
 * .pkBetween(
 *      new TableStorePkBuilder().add("pk", startPkValue).build(),
 *      new TableStorePkBuilder().add("pk", endPkValue).build())
 * .rowQuery();
 * }</pre>
 * <p>
 * <h3>Put</h3>
 * <pre> {@code
 * // 构造主键
 * PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
 * primaryKeyBuilder.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(pkValue));
 * PrimaryKey primaryKey = primaryKeyBuilder.build();
 * RowPutChange rowPutChange = new RowPutChange("table", primaryKey);
 * // 期望原行存在 , 且Col0的值大于100时写入
 * Condition condition = new Condition(RowExistenceExpectation.EXPECT_EXIST);
 * condition.setColumnCondition(new SingleColumnValueCondition("Col0",
 * SingleColumnValueCondition.CompareOperator.GREATER_THAN, ColumnValue.fromLong(100)));
 * rowPutChange.setCondition(condition);
 * //加入一些属性列
 * long ts = System.currentTimeMillis();
 * rowPutChange.addColumn(new Column("Col0", ColumnValue.fromLong(0), ts));
 * rowPutChange.addColumn(new Column("Col0", ColumnValue.fromLong(1), ts + 1));
 * rowPutChange.addColumn(new Column("Col1", ColumnValue.fromLong(1));
 * PutRowRequest request = new PutRowRequest(rowPutChange);
 *
 * equal to:
 *
 * tableStoreTemplate
 * .put("table")
 * .add(new Column("Col0", ColumnValue.fromLong(0), ts))
 * .add(new Column("Col0", ColumnValue.fromLong(1), ts + 1))
 * .add(new Column("Col1", ColumnValue.fromLong(1)))
 * .where(new TableStorePkBuilder().add("pk", pkValue).build())
 * .rowExist().condition(gt("Col0", ColumnValue.fromLong(100))).rowChange();
 * }</pre>
 * <p>
 * <h3>Update</h3>
 * <pre> {@code
 * // 构造主键
 * PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
 * primaryKeyBuilder.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(pkValue));
 * PrimaryKey primaryKey = primaryKeyBuilder.build();
 * RowUpdateChange rowUpdateChange = new RowUpdateChange("table", primaryKey);
 * // 期望原行存在, 且Col0的值大于100时更新
 * Condition condition = new Condition(RowExistenceExpectation.EXPECT_EXIST);
 * condition.setColumnCondition(new SingleColumnValueCondition("Col0",
 * SingleColumnValueCondition.CompareOperator.GREATER_THAN, ColumnValue.fromLong(100)));
 * rowUpdateChange.setCondition(condition);
 * // 更新一些列
 * rowUpdateChange.put(new Column("Col1", ColumnValue.fromLong(1)));
 * rowUpdateChange.put(new Column("Col2", ColumnValue.fromLong(2)));
 * // 删除某列的某一版本
 * rowUpdateChange.deleteColumn("Col10", 1465373223000L);
 * // 删除某一列
 * rowUpdateChange.deleteColumns("Col11");
 * UpdateRowRequest request = new UpdateRowRequest(rowUpdateChange);
 *
 * equal to:
 *
 * tableStoreTemplate.update("table")
 * .put(new Column("Col1", ColumnValue.fromLong(1)))
 * .put(new Column("Col2", ColumnValue.fromLong(2)))
 * .delete("Col10", 1465373223000L)
 * .delete("Col11")
 * .where(new TableStorePkBuilder().add("pk", pkValue).build())
 * .rowIgnore().condition(gt("Col0", ColumnValue.fromLong(100))).rowChange();
 * }</pre>
 * <p>
 * <h3>Delete</h3>
 * <pre> {@code
 * // 构造主键
 * PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
 * primaryKeyBuilder.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(pkValue));
 * PrimaryKey primaryKey = primaryKeyBuilder.build();
 * RowDeleteChange rowDeleteChange = new RowDeleteChange("table", primaryKey);
 * // 期望原行存在, 且Col0的值大于100时删除
 * Condition condition = new Condition(RowExistenceExpectation.EXPECT_EXIST);
 * condition.setColumnCondition(new SingleColumnValueCondition("Col0",
 * SingleColumnValueCondition.CompareOperator.GREATER_THAN, ColumnValue.fromLong(100)));
 * rowDeleteChange.setCondition(condition);
 * DeleteRowRequest request = new DeleteRowRequest(rowDeleteChange);
 *
 * equal to:
 *
 * tableStoreTemplate
 * .delete("table")
 * .where(new TableStorePkBuilder().add("pk", pkValue).build())
 * .rowExist().condition(gt("Col0", ColumnValue.fromLong(100))).rowChange();
 * }</pre>
 *
 * @author jiehong.jh
 * @date 2018/8/1
 */
public interface TableStoreOperations {

    /**
     * 读取该行所有的列，支持单行读、范围（迭代）读和批量读。默认读取最新版本，范围读默认限制每次返回100条
     *
     * @return
     */
    TableStoreSelect select();

    /**
     * 读取行的指定列，支持单行读、范围（迭代）读和批量读。默认读取最新版本，范围读默认限制每次返回100条
     *
     * @param columnNames
     * @return
     */
    TableStoreSelect select(String... columnNames);

    /**
     * RowPutChange
     *
     * @param tableName
     * @return
     */
    TableStorePut put(String tableName);

    /**
     * RowUpdateChange
     *
     * @param tableName
     * @return
     */
    TableStoreUpdate update(String tableName);

    /**
     * RowDeleteChange
     *
     * @param tableName
     * @return
     */
    TableStoreDelete delete(String tableName);
}
