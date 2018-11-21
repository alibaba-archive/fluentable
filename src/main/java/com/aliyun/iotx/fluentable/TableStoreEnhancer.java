package com.aliyun.iotx.fluentable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.BatchGetRowRequest;
import com.alicloud.openservices.tablestore.model.BatchGetRowResponse;
import com.alicloud.openservices.tablestore.model.BatchGetRowResponse.RowResult;
import com.alicloud.openservices.tablestore.model.GetRowRequest;
import com.alicloud.openservices.tablestore.model.GetRowResponse;
import com.alicloud.openservices.tablestore.model.MultiRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.SingleRowQueryCriteria;
import com.aliyun.iotx.fluentable.annotation.TableStoreAnnotationParser;
import com.aliyun.iotx.fluentable.api.GenericCounter;
import com.aliyun.iotx.fluentable.api.GenericList;
import com.aliyun.iotx.fluentable.api.GenericLock;
import com.aliyun.iotx.fluentable.api.GenericQueue;
import com.aliyun.iotx.fluentable.api.JsonHelper;
import com.aliyun.iotx.fluentable.util.TableStorePkBuilder;
import org.springframework.beans.factory.annotation.Autowired;

import static com.aliyun.iotx.fluentable.TableStoreList.COLUMN_ID;

/**
 * @author jiehong.jh
 * @date 2018/9/25
 */
public class TableStoreEnhancer implements FluentableService {

    private static final int ROW_LIMIT = 100;

    private SyncClientInterface syncClient;

    @Autowired
    private BridgeProperties properties;
    @Autowired
    private TableStoreOperations tableStoreTemplate;
    @Autowired
    private TableStoreAnnotationParser annotationParser;

    @Autowired
    private JsonHelper jsonHelper;

    public TableStoreEnhancer(SyncClientInterface syncClient) {
        this.syncClient = syncClient;
    }

    @Override
    public SyncClientInterface syncClient() {
        return syncClient;
    }

    @Override
    public GenericCounter opsForCounter(String key) {
        return new TableStoreCounter(key, this);
    }

    @Override
    public GenericLock opsForLock(String name) {
        return new TableStoreLock(name, this);
    }

    @Override
    public <T> GenericQueue<T> opsForQueue(String key) {
        return new TableStoreQueue<>(key, this);
    }

    @Override
    public <T> GenericList<T> opsForList(String key) {
        SingleRowQueryCriteria criteria = tableStoreTemplate.select().from(properties.getListInfo())
            .where().pkEqual(listKey(key)).rowQuery();
        GetRowResponse response = syncClient.getRow(new GetRowRequest(criteria));
        return new TableStoreList<>(key, response.getRow(), this);
    }

    @Override
    public <T> Map<String, GenericList<T>> opsForMultiList(List<String> keys) {
        if (keys.size() > ROW_LIMIT) {
            throw new IllegalArgumentException("批量获取数据过大");
        }
        List<PrimaryKey> keyList = keys.stream().map(this::listKey).collect(Collectors.toList());
        MultiRowQueryCriteria criteria = tableStoreTemplate.select().from(properties.getListInfo())
            .where().pkIn(keyList).rowQuery();
        BatchGetRowRequest request = new BatchGetRowRequest();
        request.addMultiRowQueryCriteria(criteria);
        BatchGetRowResponse response = syncClient.batchGetRow(request);
        List<RowResult> rowResults = response.getSucceedRows();
        int size = keys.size();
        Map<String, GenericList<T>> map = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String key = keys.get(i);
            Row row = rowResults.get(i).getRow();
            map.put(key, new TableStoreList<>(key, row, this));
        }
        return map;
    }

    private PrimaryKey listKey(String key) {
        return new TableStorePkBuilder().add(COLUMN_ID, key).build();
    }

    public SyncClientInterface getSyncClient() {
        return syncClient;
    }

    public BridgeProperties getProperties() {
        return properties;
    }

    public TableStoreOperations getTableStoreTemplate() {
        return tableStoreTemplate;
    }

    public TableStoreAnnotationParser getAnnotationParser() {
        return annotationParser;
    }

    public JsonHelper getJsonHelper() {
        return jsonHelper;
    }
}
