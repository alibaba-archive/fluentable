package com.aliyun.iotx.fluentable;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.DeleteRowRequest;
import com.alicloud.openservices.tablestore.model.GetRowRequest;
import com.alicloud.openservices.tablestore.model.GetRowResponse;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.RowDeleteChange;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.SingleRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;
import com.aliyun.iotx.fluentable.api.GenericLock;
import com.aliyun.iotx.fluentable.operation.TableStorePut;
import com.aliyun.iotx.fluentable.util.TableStorePkBuilder;
import lombok.extern.slf4j.Slf4j;

import static com.aliyun.iotx.fluentable.util.TableStoreConditions.eq;

/**
 * @author jiehong.jh
 * @date 2018/9/19
 */
@Slf4j
public class TableStoreLock implements GenericLock {

    private static final String ERROR_CODE = "OTSConditionCheckFail";

    private static final String COLUMN_ID = "id";
    private static final String COLUMN_VERSION = "version";
    private static final String COLUMN_EXPIRE_TIME = "expireTime";
    private static final String COLUMN_TICKET = "ticket";

    private Lock lock = new ReentrantLock();

    private String ticket;
    private long expireTime;
    private long version;

    private PrimaryKey key;
    private BridgeProperties properties;
    private TableStoreOperations tableStoreTemplate;
    private SyncClientInterface syncClient;

    public TableStoreLock(String key, TableStoreEnhancer enhancer) {
        this.key = new TableStorePkBuilder().add(COLUMN_ID, key).build();
        this.ticket = UUID.randomUUID().toString();
        this.properties = enhancer.getProperties();
        this.tableStoreTemplate = enhancer.getTableStoreTemplate();
        this.syncClient = enhancer.getSyncClient();
    }

    /**
     * 获取分布式锁
     *
     * @param leaseTime
     * @param unit
     * @return
     */
    private boolean tryLockCluster(long leaseTime, TimeUnit unit) {
        try {
            SingleRowQueryCriteria criteria = tableStoreTemplate.select().from(properties.getLock())
                .where().pkEqual(key).rowQuery();
            GetRowResponse response = syncClient.getRow(new GetRowRequest(criteria));
            Row row = response.getRow();
            long current = System.currentTimeMillis();
            RowPutChange putChange;
            if (row != null) {
                if (getExpireTime(row) > current) {
                    return ticket.equals(getTicket(row));
                }
                expireTime = unit.toMillis(leaseTime) + current;
                version = getVersion(row) + 1;
                SingleColumnValueCondition condition = eq(COLUMN_VERSION, ColumnValue.fromLong(version - 1));
                putChange = put().where(key).rowExist().condition(condition).rowChange();
            } else {
                expireTime = unit.toMillis(leaseTime) + current;
                version = 1L;
                putChange = put().where(key).rowNotExist().rowChange();
            }
            syncClient.putRow(new PutRowRequest(putChange));
            return true;
        } catch (TableStoreException ex) {
            if (!ERROR_CODE.equals(ex.getErrorCode())) {
                log.info("lock failure: {}", ex.getErrorCode());
            }
        } catch (Exception ex) {
            log.error("try lock error.", ex);
        }
        return false;
    }

    private TableStorePut put() {
        return tableStoreTemplate.put(properties.getLock())
            .add(COLUMN_EXPIRE_TIME, ColumnValue.fromLong(expireTime))
            .add(COLUMN_VERSION, ColumnValue.fromLong(version))
            .add(COLUMN_TICKET, ColumnValue.fromString(ticket));
    }

    @Override
    public boolean tryLock(long leaseTime, TimeUnit unit) {
        if (lock.tryLock()) {
            if (tryLockCluster(leaseTime, unit)) {
                return true;
            }
            lock.unlock();
        }
        return false;
    }

    @Override
    public boolean tryLock(long waitTime, long leaseTime, TimeUnit unit) throws InterruptedException {
        final long start = System.currentTimeMillis();
        final long breakTime = unit.toMillis(waitTime) + start;
        if (lock.tryLock(waitTime, unit)) {
            int loop = 0;
            boolean isLock;
            do {
                isLock = tryLockCluster(leaseTime, unit);
            } while (!isLock && hasNext(start, breakTime, ++loop));
            if (isLock) {
                return true;
            }
            lock.unlock();
        }
        return false;
    }

    private boolean hasNext(long start, long breakTime, int loop) {
        long current = System.currentTimeMillis();
        long exec = (current - start) / loop;
        return current + exec < breakTime;
    }

    @Override
    public void lock(long leaseTime, TimeUnit unit) {
        lock.lock();
        while (!tryLockCluster(leaseTime, unit)) {}
    }

    @Override
    public void unlock() {
        if (expireTime > System.currentTimeMillis()) {
            RowDeleteChange deleteChange = tableStoreTemplate.delete(properties.getLock()).where(key)
                .rowExist().condition(eq(COLUMN_VERSION, ColumnValue.fromLong(version))).rowChange();
            try {
                syncClient.deleteRow(new DeleteRowRequest(deleteChange));
            } catch (Exception ex) {
                log.error("unlock error.", ex);
            }
        }
        lock.unlock();
    }

    private long getVersion(Row row) {
        return row.getLatestColumn(COLUMN_VERSION).getValue().asLong();
    }

    private long getExpireTime(Row row) {
        return row.getLatestColumn(COLUMN_EXPIRE_TIME).getValue().asLong();
    }

    private String getTicket(Row row) {
        return row.getLatestColumn(COLUMN_TICKET).getValue().asString();
    }
}
