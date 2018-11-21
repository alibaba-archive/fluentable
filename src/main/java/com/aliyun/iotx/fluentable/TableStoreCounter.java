package com.aliyun.iotx.fluentable;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.DeleteRowRequest;
import com.alicloud.openservices.tablestore.model.GetRowRequest;
import com.alicloud.openservices.tablestore.model.GetRowResponse;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.ReturnType;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.RowDeleteChange;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.alicloud.openservices.tablestore.model.SingleRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.UpdateRowRequest;
import com.alicloud.openservices.tablestore.model.UpdateRowResponse;
import com.aliyun.iotx.fluentable.api.GenericCounter;
import com.aliyun.iotx.fluentable.util.TableStorePkBuilder;
import lombok.extern.slf4j.Slf4j;

import static com.aliyun.iotx.fluentable.util.TableStoreConditions.eq;

/**
 * @author jiehong.jh
 * @date 2018/9/23
 */
@Slf4j
public class TableStoreCounter implements GenericCounter {

    private static final String COLUMN_ID = "id";
    private static final String COLUMN_VALUE = "value";

    private static final String ERROR_CODE = "OTSConditionCheckFail";

    private PrimaryKey primaryKey;
    private BridgeProperties properties;
    private TableStoreOperations tableStoreTemplate;
    private SyncClientInterface syncClient;

    public TableStoreCounter(String key, TableStoreEnhancer enhancer) {
        this.primaryKey = new TableStorePkBuilder().add(COLUMN_ID, key).build();
        this.properties = enhancer.getProperties();
        this.tableStoreTemplate = enhancer.getTableStoreTemplate();
        this.syncClient = enhancer.getSyncClient();
    }

    @Override
    public long get() {
        SingleRowQueryCriteria criteria = tableStoreTemplate.select().from(properties.getCounter())
            .where().pkEqual(primaryKey).rowQuery();
        GetRowResponse response = syncClient.getRow(new GetRowRequest(criteria));
        Row row = response.getRow();
        if (row == null) {
            return 0L;
        }
        return getValue(row);
    }

    private long getValue(Row row) {
        return row.getLatestColumn(COLUMN_VALUE).getValue().asLong();
    }

    @Override
    public boolean set(long newValue) {
        RowPutChange putChange = tableStoreTemplate.put(properties.getCounter())
            .add(COLUMN_VALUE, ColumnValue.fromLong(newValue))
            .where(primaryKey).rowIgnore().rowChange();
        try {
            syncClient.putRow(new PutRowRequest(putChange));
            return true;
        } catch (Exception ex) {
            log.error("set value: {}", newValue, ex);
        }
        return false;
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        RowUpdateChange updateChange = tableStoreTemplate.update(properties.getCounter())
            .put(COLUMN_VALUE, ColumnValue.fromLong(update)).where(primaryKey).rowExist()
            .condition(eq(COLUMN_VALUE, ColumnValue.fromLong(expect))).rowChange();
        try {
            syncClient.updateRow(new UpdateRowRequest(updateChange));
            return true;
        } catch (TableStoreException ex) {
            if (!ERROR_CODE.equals(ex.getErrorCode())) {
                log.info("compare value: {} and set: {} failure: {}", expect, update, ex.getErrorCode());
            }
        } catch (Exception ex) {
            log.error("compare value: {} and set: {}", expect, update, ex);
        }
        return false;
    }

    @Override
    public long getAndIncrement() {
        return getAndAdd(1);
    }

    @Override
    public long getAndDecrement() {
        return getAndAdd(-1);
    }

    @Override
    public long incrementAndGet() {
        return addAndGet(1);
    }

    @Override
    public long decrementAndGet() {
        return addAndGet(-1);
    }

    @Override
    public long getAndAdd(int delta) {
        return addAndGet(delta) - delta;
    }

    @Override
    public long addAndGet(int delta) {
        RowUpdateChange updateChange = tableStoreTemplate.update(properties.getCounter())
            .inc(COLUMN_VALUE, delta).where(primaryKey).rowIgnore().rowChange();
        updateChange.addReturnColumn(COLUMN_VALUE);
        updateChange.setReturnType(ReturnType.RT_AFTER_MODIFY);
        UpdateRowResponse response = syncClient.updateRow(new UpdateRowRequest(updateChange));
        return getValue(response.getRow());
    }

    @Override
    public void delete() {
        RowDeleteChange deleteChange = tableStoreTemplate.delete(properties.getCounter())
            .where(primaryKey).rowIgnore().rowChange();
        syncClient.deleteRow(new DeleteRowRequest(deleteChange));
    }
}
