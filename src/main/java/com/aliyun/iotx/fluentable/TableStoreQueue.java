package com.aliyun.iotx.fluentable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.BatchGetRowRequest;
import com.alicloud.openservices.tablestore.model.BatchGetRowResponse;
import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse;
import com.alicloud.openservices.tablestore.model.DeleteRowRequest;
import com.alicloud.openservices.tablestore.model.GetRowRequest;
import com.alicloud.openservices.tablestore.model.GetRowResponse;
import com.alicloud.openservices.tablestore.model.MultiRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.PutRowResponse;
import com.alicloud.openservices.tablestore.model.RangeIteratorParameter;
import com.alicloud.openservices.tablestore.model.ReturnType;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.RowDeleteChange;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.alicloud.openservices.tablestore.model.SingleRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.UpdateRowRequest;
import com.aliyun.iotx.fluentable.annotation.TableStoreAnnotationParser;
import com.aliyun.iotx.fluentable.api.GenericQueue;
import com.aliyun.iotx.fluentable.api.JsonHelper;
import com.aliyun.iotx.fluentable.api.QueueElement;
import com.aliyun.iotx.fluentable.model.OptionalRow;
import com.aliyun.iotx.fluentable.util.TableStorePkBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * @author jiehong.jh
 * @date 2018/9/19
 */
@Slf4j
public class TableStoreQueue<E> implements GenericQueue<E> {

    private static final String COLUMN_ID = "id";
    private static final String COLUMN_SEQUENCE = "sequence";
    private static final String COLUMN_TYPE = "type";
    private static final String COLUMN_VALUE = "value";

    private static final String ERROR_CODE = "OTSConditionCheckFail";

    /**
     * 批量写入行数限制，必须<=200
     */
    private static final int WRITE_LIMIT = 180;
    /**
     * 批量读取行数限制，必须<=100
     */
    private static final int GET_LIMIT = 80;

    private String key;
    private BridgeProperties properties;
    private TableStoreOperations tableStoreTemplate;
    private TableStoreAnnotationParser annotationParser;
    private SyncClientInterface syncClient;
    private JsonHelper jsonHelper;

    public TableStoreQueue(String key, TableStoreEnhancer enhancer) {
        this.key = key;
        this.properties = enhancer.getProperties();
        this.tableStoreTemplate = enhancer.getTableStoreTemplate();
        this.syncClient = enhancer.getSyncClient();
        this.annotationParser = enhancer.getAnnotationParser();
        this.jsonHelper = enhancer.getJsonHelper();
    }

    @Override
    public Long push(E e) {
        if (e == null) {
            throw new NullPointerException();
        }
        QueueElement<E> queueElement = new QueueElement<>();
        queueElement.setId(key);
        queueElement.setType(e.getClass().getName());
        queueElement.setValue(jsonHelper.toJson(e));
        RowPutChange rowPutChange = tableStoreTemplate.put(properties.getQueue())
            .with(annotationParser.parse(queueElement)).rowIgnore().rowChange();
        rowPutChange.setReturnType(ReturnType.RT_PK);
        try {
            PutRowResponse response = syncClient.putRow(new PutRowRequest(rowPutChange));
            Row row = response.getRow();
            if (row != null) {
                return getSequence(row);
            }
        } catch (Exception ex) {
            log.error("push element to queue", ex);
        }
        return null;
    }

    private long getSequence(Row row) {
        return row.getPrimaryKey().getPrimaryKeyColumn(COLUMN_SEQUENCE).getValue().asLong();
    }

    @Override
    public List<Long> pushAll(Collection<? extends E> c) {
        if (c == null) {
            throw new NullPointerException();
        }
        if (c == this) {
            throw new IllegalArgumentException();
        }
        boolean elementNull = c.stream().anyMatch(Objects::isNull);
        if (elementNull) {
            throw new NullPointerException();
        }
        List<Long> list = new ArrayList<>(c.size());
        int i = 1;
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        for (E e : c) {
            QueueElement<E> queueElement = new QueueElement<>();
            queueElement.setId(key);
            queueElement.setType(e.getClass().getName());
            queueElement.setValue(jsonHelper.toJson(e));
            RowPutChange rowPutChange = tableStoreTemplate.put(properties.getQueue())
                .with(annotationParser.parse(queueElement)).rowIgnore().rowChange();
            batchWriteRowRequest.addRowChange(rowPutChange);
            rowPutChange.setReturnType(ReturnType.RT_PK);
            if (i++ % WRITE_LIMIT == 0) {
                try {
                    syncClient.batchWriteRow(batchWriteRowRequest).getSucceedRows()
                        .stream()
                        .map(BatchWriteRowResponse.RowResult::getRow)
                        .forEach(row -> list.add(getSequence(row)));
                } catch (Exception ex) {
                    log.error("push collection to queue", ex);
                } finally {
                    batchWriteRowRequest = new BatchWriteRowRequest();
                }
            }
        }
        if (!batchWriteRowRequest.isEmpty()) {
            try {
                syncClient.batchWriteRow(batchWriteRowRequest).getSucceedRows()
                    .stream()
                    .map(BatchWriteRowResponse.RowResult::getRow)
                    .forEach(row -> list.add(getSequence(row)));
            } catch (Exception ex) {
                log.error("push collection to queue", ex);
            }
        }
        return list;
    }

    @Override
    public QueueElement<E> peek() {
        Iterator<QueueElement<E>> iterator = iterator(1);
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @Override
    public List<QueueElement<E>> peek(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("The size must be greater than 0.");
        }
        List<QueueElement<E>> list = new ArrayList<>(size);
        Iterator<QueueElement<E>> iterator = iterator(size);
        while (iterator.hasNext() && size-- > 0) {
            list.add(iterator.next());
        }
        return list;
    }

    @Override
    public Iterator<QueueElement<E>> iterator() {
        return iterator(-1);
    }

    private Iterator<QueueElement<E>> iterator(int maxCount) {
        PrimaryKey inclusive = new TableStorePkBuilder().add(COLUMN_ID, key).addMin(COLUMN_SEQUENCE).build();
        PrimaryKey exclusive = new TableStorePkBuilder().add(COLUMN_ID, key).addMax(COLUMN_SEQUENCE).build();
        RangeIteratorParameter parameter = tableStoreTemplate.select().from(properties.getQueue())
            .where().pkBetween(inclusive, exclusive).maxCount(maxCount).rowQuery();
        Iterator<Row> iterator = syncClient.createRangeIterator(parameter);
        return new Iterator<QueueElement<E>>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public QueueElement<E> next() {
                Row row = iterator.next();
                long sequence = getSequence(row);
                String type = row.getLatestColumn(COLUMN_TYPE).getValue().asString();
                String value = row.getLatestColumn(COLUMN_VALUE).getValue().asString();
                E e = jsonHelper.fromJson(value, type);
                QueueElement<E> queueElement = new QueueElement<>();
                queueElement.setId(key);
                queueElement.setSequence(sequence);
                queueElement.setType(type);
                queueElement.setValue(value);
                queueElement.setObject(e);
                return queueElement;
            }

            @Override
            public void remove() {
                iterator.remove();
            }
        };
    }

    @Override
    public E get(Long sequence) {
        if (sequence == null) {
            throw new NullPointerException();
        }
        PrimaryKey primaryKey = new TableStorePkBuilder().add(COLUMN_ID, key).add(COLUMN_SEQUENCE, sequence).build();
        SingleRowQueryCriteria criteria = tableStoreTemplate.select().from(properties.getQueue())
            .where().pkEqual(primaryKey).rowQuery();
        GetRowResponse response = syncClient.getRow(new GetRowRequest(criteria));
        Row row = response.getRow();
        if (row == null) {
            return null;
        }
        String type = row.getLatestColumn(COLUMN_TYPE).getValue().asString();
        String value = row.getLatestColumn(COLUMN_VALUE).getValue().asString();
        return jsonHelper.fromJson(value, type);
    }

    @Override
    public Map<Long, E> getAll(List<Long> sequenceList) {
        if (sequenceList == null || sequenceList.isEmpty()) {
            return Collections.emptyMap();
        }
        List<PrimaryKey> primaryKeys = sequenceList.stream()
            .filter(Objects::nonNull)
            .map(sequence -> new TableStorePkBuilder()
                .add(COLUMN_ID, key)
                .add(COLUMN_SEQUENCE, sequence).build())
            .collect(Collectors.toList());
        Map<Long, E> result = new HashMap<>(primaryKeys.size());
        int i = 1;
        List<PrimaryKey> parts = new ArrayList<>(GET_LIMIT);
        for (PrimaryKey primaryKey : primaryKeys) {
            parts.add(primaryKey);
            if (i++ % GET_LIMIT == 0) {
                try {
                    batchGet(result, parts);
                } catch (Exception ex) {
                    log.error("batch get from queue", ex);
                } finally {
                    parts.clear();
                }
            }
        }
        if (!parts.isEmpty()) {
            try {
                batchGet(result, parts);
            } catch (Exception ex) {
                log.error("batch get from queue", ex);
            }
        }
        return result;
    }

    private void batchGet(Map<Long, E> result, List<PrimaryKey> parts) {
        MultiRowQueryCriteria criteria = tableStoreTemplate.select().from(properties.getQueue())
            .where().pkIn(parts).rowQuery();
        BatchGetRowRequest batchGetRowRequest = new BatchGetRowRequest();
        batchGetRowRequest.addMultiRowQueryCriteria(criteria);
        BatchGetRowResponse response = syncClient.batchGetRow(batchGetRowRequest);
        response.getSucceedRows().stream()
            .map(BatchGetRowResponse.RowResult::getRow)
            .forEach(row -> {
                String type = row.getLatestColumn(COLUMN_TYPE).getValue().asString();
                String value = row.getLatestColumn(COLUMN_VALUE).getValue().asString();
                result.put(getSequence(row), jsonHelper.fromJson(value, type));
            });
    }

    @Override
    public boolean set(Long sequence, E element) {
        if (sequence == null || element == null) {
            throw new NullPointerException();
        }
        QueueElement<E> queueElement = new QueueElement<>();
        queueElement.setId(key);
        queueElement.setSequence(sequence);
        queueElement.setType(element.getClass().getName());
        queueElement.setValue(jsonHelper.toJson(element));
        OptionalRow optionalRow = annotationParser.parse(queueElement, false);
        RowUpdateChange updateChange = tableStoreTemplate.update(properties.getQueue())
            .with(optionalRow).rowExist().rowChange();
        try {
            syncClient.updateRow(new UpdateRowRequest(updateChange));
            return true;
        } catch (TableStoreException ex) {
            if (!ERROR_CODE.equals(ex.getErrorCode())) {
                log.info("set element failure: {}", ex.getErrorCode());
            }
        } catch (Exception ex) {
            log.error("set element to queue sequence: {}", sequence, ex);
        }
        return false;
    }

    @Override
    public boolean remove(Long sequence) {
        if (sequence == null) {
            throw new NullPointerException();
        }
        PrimaryKey primaryKey = new TableStorePkBuilder().add(COLUMN_ID, key).add(COLUMN_SEQUENCE, sequence).build();
        RowDeleteChange deleteChange = tableStoreTemplate.delete(properties.getQueue())
            .where(primaryKey).rowExist().rowChange();
        try {
            syncClient.deleteRow(new DeleteRowRequest(deleteChange));
            return true;
        } catch (TableStoreException ex) {
            if (!ERROR_CODE.equals(ex.getErrorCode())) {
                log.info("remove element failure: {}", ex.getErrorCode());
            }
        } catch (Exception ex) {
            log.error("delete row error.", ex);
        }
        return false;
    }

    @Override
    public void removeAll(List<Long> sequenceList) {
        if (sequenceList == null || sequenceList.isEmpty()) {
            return;
        }
        int i = 1;
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        for (Long sequence : sequenceList) {
            PrimaryKey primaryKey = new TableStorePkBuilder()
                .add(COLUMN_ID, key).add(COLUMN_SEQUENCE, sequence).build();
            RowDeleteChange deleteChange = tableStoreTemplate.delete(properties.getQueue())
                .where(primaryKey).rowExist().rowChange();
            batchWriteRowRequest.addRowChange(deleteChange);
            if (i++ % WRITE_LIMIT == 0) {
                try {
                    syncClient.batchWriteRow(batchWriteRowRequest);
                } catch (Exception ex) {
                    log.error("batch remove from queue", ex);
                } finally {
                    batchWriteRowRequest = new BatchWriteRowRequest();
                }
            }
        }
        if (!batchWriteRowRequest.isEmpty()) {
            try {
                syncClient.batchWriteRow(batchWriteRowRequest);
            } catch (Exception ex) {
                log.error("batch remove from queue", ex);
            }
        }
    }

    @Override
    public void delete() {
        PrimaryKey inclusive = new TableStorePkBuilder().add(COLUMN_ID, key).addMin(COLUMN_SEQUENCE).build();
        PrimaryKey exclusive = new TableStorePkBuilder().add(COLUMN_ID, key).addMax(COLUMN_SEQUENCE).build();
        RangeIteratorParameter parameter = tableStoreTemplate.select().from(properties.getQueue())
            .where().pkBetween(inclusive, exclusive).rowQuery();
        int i = 1;
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        Iterator<Row> iterator = syncClient.createRangeIterator(parameter);
        while (iterator.hasNext()) {
            RowDeleteChange deleteChange = tableStoreTemplate.delete(properties.getQueue())
                .where(iterator.next().getPrimaryKey())
                .rowIgnore().rowChange();
            batchWriteRowRequest.addRowChange(deleteChange);
            if (i++ % WRITE_LIMIT == 0) {
                try {
                    syncClient.batchWriteRow(batchWriteRowRequest);
                } catch (Exception ex) {
                    log.error("delete queue", ex);
                } finally {
                    batchWriteRowRequest = new BatchWriteRowRequest();
                }
            }
        }
        if (!batchWriteRowRequest.isEmpty()) {
            try {
                syncClient.batchWriteRow(batchWriteRowRequest);
            } catch (Exception ex) {
                log.error("delete queue", ex);
            }
        }
    }
}
