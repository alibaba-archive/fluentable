package com.aliyun.iotx.fluentable;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.aliyun.iotx.fluentable.util.TableStorePkBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static com.aliyun.iotx.fluentable.util.TableStoreConditions.eq;

/**
 * @author jiehong.jh
 * @date 2018/9/20
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = JunitConfiguration.class)
public class ConditionTest {

    private static final String TABLE_NAME = "hlo";
    @Autowired
    private TableStoreOperations tableStoreTemplate;
    @Autowired
    private SyncClientInterface syncClient;

    @Test
    public void put() {
        RowPutChange rowPutChange = tableStoreTemplate.put(TABLE_NAME)
            .add("inc", ColumnValue.fromLong(1L))
            .where(new TableStorePkBuilder().add("id", 1).build())
            .rowNotExist().rowChange();
        syncClient.putRow(new PutRowRequest(rowPutChange));
    }

    @Test
    public void multiPut() throws InterruptedException {
        int nThreads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        for (int i = 0; i < nThreads; i++) {
            executor.execute(() -> {
                RowPutChange rowPutChange = tableStoreTemplate.put(TABLE_NAME)
                    .add("inc", ColumnValue.fromLong(4L))
                    .where(new TableStorePkBuilder().add("id", 1).build())
                    .rowExist().condition(eq("inc", ColumnValue.fromLong(3L))).rowChange();
                try {
                    syncClient.putRow(new PutRowRequest(rowPutChange));
                    System.out.println(Thread.currentThread().getName() + "--> success");
                } catch (TableStoreException ex) {
                    if ("OTSConditionCheckFail".equals(ex.getErrorCode())) {
                        System.out.println(Thread.currentThread().getName() + "--> failure");
                    }
                }
            });
        }
        TimeUnit.SECONDS.sleep(5);
    }
}
