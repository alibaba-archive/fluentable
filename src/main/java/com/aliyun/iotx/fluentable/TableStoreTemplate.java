package com.aliyun.iotx.fluentable;

import com.aliyun.iotx.fluentable.operation.TableStoreDelete;
import com.aliyun.iotx.fluentable.operation.TableStorePut;
import com.aliyun.iotx.fluentable.operation.TableStoreSelect;
import com.aliyun.iotx.fluentable.operation.TableStoreUpdate;

/**
 * @author jiehong.jh
 * @date 2018/7/31
 */
public class TableStoreTemplate implements TableStoreOperations {

    @Override
    public TableStoreSelect select() {
        return new TableStoreSelect();
    }

    @Override
    public TableStoreSelect select(String... columnNames) {
        return new TableStoreSelect(columnNames);
    }

    @Override
    public TableStorePut put(String tableName) {
        return new TableStorePut(tableName);
    }

    @Override
    public TableStoreUpdate update(String tableName) {
        return new TableStoreUpdate(tableName);
    }

    @Override
    public TableStoreDelete delete(String tableName) {
        return new TableStoreDelete(tableName);
    }
}
