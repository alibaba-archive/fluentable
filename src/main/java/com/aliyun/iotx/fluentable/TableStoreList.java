package com.aliyun.iotx.fluentable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.DeleteRowRequest;
import com.alicloud.openservices.tablestore.model.GetRowRequest;
import com.alicloud.openservices.tablestore.model.GetRowResponse;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.model.RowDeleteChange;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.SingleRowQueryCriteria;
import com.aliyun.iotx.fluentable.api.FlushCollisionException;
import com.aliyun.iotx.fluentable.api.GenericList;
import com.aliyun.iotx.fluentable.api.JsonHelper;
import com.aliyun.iotx.fluentable.operation.TableStorePut;
import com.aliyun.iotx.fluentable.util.TableStorePkBuilder;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.iotx.fluentable.util.TableStoreConditions.eq;

/**
 * @author jiehong.jh
 * @date 2018/9/14
 */
public class TableStoreList<E> implements GenericList<E> {

    public static final String COLUMN_ID = "id";
    private static final String COLUMN_VERSION = "version";
    private static final String COLUMN_PARTS = "parts";
    private static final String COLUMN_TYPES = "types";
    private static final String COLUMN_EXPIRE_TIME = "expireTime";

    private static final String COLUMN_PART = "part";

    private static final long PERMANENT = -1L;
    private static final int NULL = -1;
    private static final int NOT_NULL = 1;
    private static final String COLUMN_PREFIX = "COL";
    /**
     * 单行列数限制
     */
    private static final int LIMIT = 768;
    /**
     * 单行列数推荐
     */
    private static final int BALANCE = 512;
    /**
     * 列表信息
     */
    private ListInfo info = new ListInfo();
    /**
     * 列表Root（责任链）
     */
    private ListPart root;
    private String key;
    private BridgeProperties properties;
    private TableStoreOperations tableStoreTemplate;
    private SyncClientInterface syncClient;
    private JsonHelper jsonHelper;

    /**
     * @param key      列表主键
     * @param row      列表信息行
     * @param enhancer
     */
    public TableStoreList(String key, Row row, TableStoreEnhancer enhancer) {
        this.key = key;
        this.properties = enhancer.getProperties();
        this.tableStoreTemplate = enhancer.getTableStoreTemplate();
        this.syncClient = enhancer.getSyncClient();
        this.jsonHelper = enhancer.getJsonHelper();
        if (row == null) {
            root = new ListPart(0, 0, false);
        } else {
            initialize(row);
        }
    }

    private void initialize(Row row) {
        info.version = row.getLatestColumn(COLUMN_VERSION).getValue().asLong();
        // java.lang.String,java.lang.Integer
        String types = row.getLatestColumn(COLUMN_TYPES).getValue().asString();
        info.typeList.addAll(Arrays.asList(StringUtils.split(types, ",")));
        // 100,100,23
        String parts = row.getLatestColumn(COLUMN_PARTS).getValue().asString();
        String[] partArray = StringUtils.split(parts, ",");
        long expireTime = row.getLatestColumn(COLUMN_EXPIRE_TIME).getValue().asLong();
        if (expireTime != PERMANENT) {
            // 过期无效
            if (expireTime < System.currentTimeMillis()) {
                lazyRoot(Arrays.stream(partArray).map(s -> 0).collect(Collectors.toList()), true);
                return;
            }
            info.expireTime = expireTime;
        }
        lazyRoot(Arrays.stream(partArray).map(Integer::valueOf).collect(Collectors.toList()), false);
    }

    private void putIfAbsent(E e) {
        if (e != null) {
            String name = e.getClass().getName();
            if (!info.typeList.contains(name)) {
                info.typeList.add(name);
            }
        }
    }

    private void putIfAbsent(Collection<? extends E> c) {
        if (c == null || c.isEmpty()) {
            return;
        }
        c.forEach(e -> {
            String name = e.getClass().getName();
            if (!info.typeList.contains(name)) {
                info.typeList.add(name);
            }
        });
    }

    private void lazyRoot(List<Integer> parts, boolean flush) {
        root = new ListPart(0, parts.get(0), flush);
        lazyRoot(parts, flush, 1, root);
    }

    private void lazyRoot(List<Integer> parts, boolean flush, int index, ListPart parent) {
        if (index == parts.size()) {
            return;
        }
        ListPart part = new ListPart(index, parts.get(index), flush);
        parent.nextPart = part;
        lazyRoot(parts, flush, index + 1, part);
    }

    private List<E> fetch(int part) {
        PrimaryKey primaryKey = new TableStorePkBuilder().add(COLUMN_ID, key).add(COLUMN_PART, part).build();
        SingleRowQueryCriteria criteria = tableStoreTemplate.select().from(properties.getList())
            .where().pkEqual(primaryKey).rowQuery();
        GetRowResponse response = syncClient.getRow(new GetRowRequest(criteria));
        Column[] columns = response.getRow().getColumns();
        List<Element> elements = new ArrayList<>(columns.length);
        for (Column column : columns) {
            // order,length|type,json
            String columnValue = column.getValue().asString();
            int index = columnValue.indexOf("|");
            String orderAndLength = columnValue.substring(0, index);
            int split = orderAndLength.indexOf(",");
            int order = Integer.parseInt(orderAndLength.substring(0, split));
            // length为-1表示值为null
            int length = Integer.parseInt(orderAndLength.substring(split + 1));
            Element element = new Element();
            element.order = order;
            if (length != NULL) {
                String typeAndValue = columnValue.substring(index + 1);
                split = typeAndValue.indexOf(",");
                int type = Integer.parseInt(typeAndValue.substring(0, split));
                String json = typeAndValue.substring(split + 1);
                element.value = jsonHelper.fromJson(json, info.typeList.get(type));
            }
            elements.add(element);
        }
        return elements.stream().sorted().map(element -> element.value).collect(Collectors.toList());
    }

    @Override
    public int size() {
        return root.size();
    }

    @Override
    public boolean isEmpty() {
        return root.size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        return root.contains(o);
    }

    @Override
    public Iterator<E> iterator() {
        return new LazyIterator();
    }

    @Override
    public boolean add(E e) {
        return root.add(e);
    }

    @Override
    public boolean remove(Object o) {
        return root.remove(o);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        return root.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        return root.addAll(index, c);
    }

    @Override
    public void clear() {
        root.clear();
    }

    @Override
    public E get(int index) {
        return root.get(index);
    }

    @Override
    public E set(int index, E element) {
        return root.set(index, element);
    }

    @Override
    public void add(int index, E element) {
        root.add(index, element);
    }

    @Override
    public E remove(int index) {
        return root.remove(index);
    }

    @Override
    public int indexOf(Object o) {
        return root.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return root.lastIndexOf(o);
    }

    @Override
    public void delete() {
        if (info.version == null) {
            return;
        }
        PrimaryKey primaryKey = new TableStorePkBuilder().add(COLUMN_ID, key).build();
        RowDeleteChange rowChange = tableStoreTemplate.delete(properties.getListInfo())
            .where(primaryKey).rowExist().rowChange();
        try {
            syncClient.deleteRow(new DeleteRowRequest(rowChange));
        } catch (TableStoreException ex) {
            throw new FlushCollisionException(ex);
        }
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        ListPart part = root;
        while (part != null) {
            batchWriteRowRequest.addRowChange(tableStoreTemplate.delete(properties.getList())
                .where(new TableStorePkBuilder().add(COLUMN_ID, key).add(COLUMN_PART, part.part).build())
                .rowIgnore().rowChange());
            part = part.nextPart;
        }
        if (!batchWriteRowRequest.isEmpty()) {
            syncClient.batchWriteRow(batchWriteRowRequest);
        }
    }

    @Override
    public boolean expire(long timeToLive, TimeUnit timeUnit) {
        info.flush = true;
        info.expireTime = System.currentTimeMillis() + timeUnit.toMillis(timeToLive);
        return true;
    }

    @Override
    public void flush() throws FlushCollisionException {
        if (!root.isFlush() && !info.flush) {
            return;
        }
        root.adapt(new ArrayList<>());
        PrimaryKey primaryKey = new TableStorePkBuilder().add(COLUMN_ID, key).build();
        TableStorePut put = tableStoreTemplate.put(properties.getListInfo());
        if (info.expireTime == null) {
            put.add(COLUMN_EXPIRE_TIME, ColumnValue.fromLong(PERMANENT));
        } else {
            put.add(COLUMN_EXPIRE_TIME, ColumnValue.fromLong(info.expireTime));
        }
        put.add(COLUMN_PARTS, ColumnValue.fromString(root.parts()));
        put.add(COLUMN_TYPES, ColumnValue.fromString(String.join(",", info.typeList)));
        RowPutChange rowPutChange;
        if (info.version == null) {
            rowPutChange = put.add(COLUMN_VERSION, ColumnValue.fromLong(1L))
                .where(primaryKey).rowNotExist().rowChange();
        } else {
            rowPutChange = put.add(COLUMN_VERSION, ColumnValue.fromLong(info.version + 1)).where(primaryKey)
                .rowExist().condition(eq(COLUMN_VERSION, ColumnValue.fromLong(info.version))).rowChange();
        }
        try {
            syncClient.putRow(new PutRowRequest(rowPutChange));
        } catch (TableStoreException ex) {
            throw new FlushCollisionException(ex);
        }
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        ListPart part = root;
        while (part != null) {
            if (part.flush) {
                batchWriteRowRequest.addRowChange(write(part));
            }
            part = part.nextPart;
        }
        if (!batchWriteRowRequest.isEmpty()) {
            syncClient.batchWriteRow(batchWriteRowRequest);
        }
    }

    private RowChange write(ListPart part) {
        PrimaryKey primaryKey = new TableStorePkBuilder().add(COLUMN_ID, key).add(COLUMN_PART, part.part).build();
        TableStorePut put = tableStoreTemplate.put(properties.getList());
        if (part.size != 0) {
            for (int i = 0; i < part.size; i++) {
                StringBuilder builder = new StringBuilder();
                builder.append(i).append(",");
                // order,length|type,json
                E e = part.list.get(i);
                if (e == null) {
                    builder.append(NULL).append("|");
                } else {
                    int type = info.typeList.indexOf(e.getClass().getName());
                    builder.append(NOT_NULL).append("|").append(type).append(",").append(jsonHelper.toJson(e));
                }
                put.add(COLUMN_PREFIX + i, ColumnValue.fromString(builder.toString()));
            }
        }
        return put.where(primaryKey).rowIgnore().rowChange();
    }

    private class LazyIterator implements Iterator<E> {

        private int size = root.size();
        private int index = 0;

        @Override
        public boolean hasNext() {
            return index < size;
        }

        @Override
        public E next() {
            return root.get(index++);
        }
    }

    private class Element implements Comparable<Element> {
        private int order;
        /**
         * 列表允许为null
         */
        private E value;

        @Override
        public int compareTo(Element o) {
            return order - o.order;
        }
    }

    private class ListInfo {
        /**
         * 是否需要刷新
         */
        boolean flush;
        /**
         * 乐观锁版本号
         */
        Long version;
        /**
         * 过期时间，值空表示不过期
         */
        Long expireTime;
        /**
         * 类型列表
         */
        List<String> typeList = new ArrayList<>();
    }

    private class ListPart {
        /**
         * 部分号
         */
        int part;
        /**
         * 列表部分大小
         */
        int size;
        /**
         * 是否需要刷新
         */
        boolean flush;
        List<E> list;

        ListPart nextPart;

        ListPart(int part, int size, boolean flush) {
            this.part = part;
            this.size = size;
            this.flush = flush;
        }

        boolean isFlush() {
            if (flush) {
                return true;
            }
            if (nextPart == null) {
                return false;
            }
            return nextPart.isFlush();
        }

        void adapt(List<E> overflow) {
            if (overflow.isEmpty()) {
                if (size <= LIMIT) {
                    if (nextPart != null) {
                        nextPart.adapt(overflow);
                    }
                } else {
                    flush = true;
                    overflow = subList(list, BALANCE, size);
                    list = subList(list, 0, BALANCE);
                    size = BALANCE;
                    if (nextPart != null) {
                        nextPart.adapt(overflow);
                    } else {
                        dynamic(overflow, part + 1, this);
                    }
                }
            } else {
                flush = true;
                doAdapt(overflow);
            }
        }

        private void doAdapt(List<E> overflow) {
            int total = overflow.size();
            if (size == 0) {
                if (total <= LIMIT) {
                    list = overflow;
                    size = total;
                    if (nextPart != null) {
                        nextPart.adapt(new ArrayList<>());
                    }
                } else {
                    list = subList(overflow, 0, BALANCE);
                    size = BALANCE;
                    overflow = subList(overflow, BALANCE, total);
                    if (nextPart != null) {
                        nextPart.adapt(overflow);
                    } else {
                        dynamic(overflow, part + 1, this);
                    }
                }
            } else {
                if (list == null) {
                    list = fetch(part);
                }
                size += total;
                overflow.addAll(list);
                list = overflow;
                if (size < LIMIT) {
                    if (nextPart != null) {
                        nextPart.adapt(new ArrayList<>());
                    }
                } else {
                    overflow = subList(list, BALANCE, size);
                    list = subList(list, 0, BALANCE);
                    size = BALANCE;
                    if (nextPart == null) {
                        dynamic(overflow, part + 1, this);
                    } else {
                        nextPart.adapt(overflow);
                    }
                }
            }
        }

        /**
         * 避免出现 <code>java.util.ConcurrentModificationException</code>
         *
         * @param src
         * @param fromIndex
         * @param toIndex
         * @return
         */
        private List<E> subList(List<E> src, int fromIndex, int toIndex) {
            return new ArrayList<>(src.subList(fromIndex, toIndex));
        }

        /**
         * 在nextPart为null的情况下，根据溢出数据进行自动扩容
         *
         * @param overflow
         * @param part
         * @param parent
         */
        private void dynamic(List<E> overflow, int part, ListPart parent) {
            int total = overflow.size();
            if (total <= BALANCE) {
                parent.nextPart = new ListPart(part, total, flush);
                parent.nextPart.list = overflow;
                return;
            }
            parent.nextPart = new ListPart(part, BALANCE, flush);
            parent.nextPart.list = subList(overflow, 0, BALANCE);
            dynamic(subList(overflow, BALANCE, total), part + 1, parent.nextPart);
        }

        String parts() {
            if (nextPart == null) {
                return size + "";
            }
            return size + "," + nextPart.parts();
        }

        int size() {
            if (nextPart == null) {
                return size;
            }
            return size + nextPart.size();
        }

        boolean contains(Object o) {
            if (size != 0) {
                if (list == null) {
                    list = fetch(part);
                }
                if (list.contains(o)) {
                    return true;
                }
            }
            if (nextPart == null) {
                return false;
            }
            return nextPart.contains(o);
        }

        boolean add(E e) {
            putIfAbsent(e);
            if (nextPart != null) {
                return nextPart.add(e);
            }
            if (size == 0) {
                list = new ArrayList<>();
            } else {
                if (list == null) {
                    list = fetch(part);
                }
            }
            if (list.add(e)) {
                size++;
                flush = true;
                return true;
            }
            return false;
        }

        boolean remove(Object o) {
            if (size != 0) {
                if (list == null) {
                    list = fetch(part);
                }
                if (list.remove(o)) {
                    size--;
                    flush = true;
                    return true;
                }
            }
            if (nextPart == null) {
                return false;
            }
            return nextPart.remove(o);
        }

        boolean addAll(Collection<? extends E> c) {
            putIfAbsent(c);
            if (nextPart != null) {
                return nextPart.addAll(c);
            }
            if (size == 0) {
                list = new ArrayList<>();
            } else {
                if (list == null) {
                    list = fetch(part);
                }
            }
            if (list.addAll(c)) {
                size += c.size();
                flush = true;
                return true;
            }
            return false;
        }

        boolean addAll(int index, Collection<? extends E> c) {
            putIfAbsent(c);
            if (index > size) {
                if (nextPart == null) {
                    throw new IndexOutOfBoundsException("part: " + part + ", size: " + size + ", index: " + index);
                }
                return nextPart.addAll(index - size, c);
            }
            if (size == 0) {
                list = new ArrayList<>();
            } else {
                if (list == null) {
                    list = fetch(part);
                }
            }
            if (list.addAll(index, c)) {
                size += c.size();
                flush = true;
                return true;
            }
            return false;
        }

        void clear() {
            if (size != 0) {
                list = new ArrayList<>();
                size = 0;
                flush = true;
            }
            if (nextPart != null) {
                nextPart.clear();
            }
        }

        E get(int index) {
            if (index >= size) {
                if (nextPart == null) {
                    throw new IndexOutOfBoundsException("part: " + part + ", size: " + size + ", index: " + index);
                }
                return nextPart.get(index - size);
            }
            if (list == null) {
                list = fetch(part);
            }
            flush = true;
            return list.get(index);
        }

        E set(int index, E element) {
            putIfAbsent(element);
            if (index >= size) {
                if (nextPart == null) {
                    throw new IndexOutOfBoundsException("part: " + part + ", size: " + size + ", index: " + index);
                }
                return nextPart.set(index - size, element);
            }
            if (list == null) {
                list = fetch(part);
            }
            flush = true;
            return list.set(index, element);
        }

        void add(int index, E element) {
            putIfAbsent(element);
            if (index > size) {
                if (nextPart == null) {
                    throw new IndexOutOfBoundsException("part: " + part + ", size: " + size + ", index: " + index);
                }
                nextPart.add(index - size, element);
            } else {
                if (size == 0) {
                    list = new ArrayList<>();
                } else {
                    if (list == null) {
                        list = fetch(part);
                    }
                }
                list.add(index, element);
                size++;
                flush = true;
            }
        }

        E remove(int index) {
            if (index >= size) {
                if (nextPart == null) {
                    throw new IndexOutOfBoundsException("part: " + part + ", size: " + size + ", index: " + index);
                }
                return nextPart.remove(index - size);
            }
            if (list == null) {
                list = fetch(part);
            }
            E e = list.remove(index);
            size--;
            flush = true;
            return e;
        }

        int indexOf(Object o) {
            if (size != 0) {
                if (list == null) {
                    list = fetch(part);
                }
                int i = list.indexOf(o);
                if (i != -1) {
                    return i;
                }
            }
            if (nextPart == null) {
                return -1;
            }
            int i = nextPart.indexOf(o);
            if (i != -1) {
                i += size;
            }
            return i;
        }

        int lastIndexOf(Object o) {
            if (nextPart != null) {
                int i = nextPart.lastIndexOf(o);
                if (i != -1) {
                    return i + size;
                }
            }
            if (size == 0) {
                return -1;
            }
            if (list == null) {
                list = fetch(part);
            }
            return list.lastIndexOf(o);
        }
    }
}
