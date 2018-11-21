package com.aliyun.iotx.fluentable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.CreateTableRequest;
import com.alicloud.openservices.tablestore.model.PrimaryKeyOption;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.TableOptions;
import com.aliyun.iotx.fluentable.api.GenericQueue;
import com.aliyun.iotx.fluentable.api.QueueElement;
import com.aliyun.iotx.fluentable.parser.ClassInfo;
import com.aliyun.iotx.fluentable.parser.FieldInfo;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author jiehong.jh
 * @date 2018/9/20
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = JunitConfiguration.class)
public class TableStoreQueueTest {

    private static final String TABLE_NAME = "queue";
    @Autowired
    private FluentableService fluentableService;
    @Autowired
    private ClassInfo classInfo;
    @Autowired
    private SyncClientInterface syncClient;

    private GenericQueue<String> queue;

    @Before
    public void setUp() {
        queue = fluentableService.opsForQueue("key1");
    }

    //@Test
    public void createTable() {
        TableMeta tableMeta = new TableMeta(TABLE_NAME);
        List<FieldInfo> infos = classInfo.getFields(QueueElement.class);
        infos.stream().filter(FieldInfo::isPrimaryKey)
            .sorted(Comparator.comparingInt(FieldInfo::getOrder))
            .forEach(info -> {
                if (info.isAutoIncrement()) {
                    tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(info.getColumnName(),
                        PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));
                } else {
                    PrimaryKeyType primaryKeyType = info.getColumnEnum().getPrimaryKeyType();
                    tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(info.getColumnName(), primaryKeyType));
                }
            });
        int timeToLive = -1;  // 永不过期，也可以设置数据有效期，过期了会自动删除
        int maxVersions = 1;  // 只保存一个版本，目前支持多版本
        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);
        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
        syncClient.createTable(request);
    }

    @Test
    public void push() {
        System.out.println(queue.push("A"));
    }

    @Test
    public void pushAll() {
        int size = 437;
        List<String> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(RandomStringUtils.randomAlphanumeric(10));
        }
        List<Long> result = queue.pushAll(list);
        result.forEach(System.out::println);
    }

    @Test
    public void peek() {
        QueueElement<String> element = queue.peek();
        if (element == null) {
            System.out.println("element is null");
        } else {
            System.out.println(element.getObject());
        }
    }

    @Test
    public void peekWithSize() {
        List<QueueElement<String>> elements = queue.peek(10);
        for (QueueElement<String> element : elements) {
            System.out.println(element.getObject());
        }
    }

    @Test
    public void iterator() {
        int count = 0;
        Iterator<QueueElement<String>> iterator = queue.iterator();
        while (iterator.hasNext()) {
            System.out.println("-->" + iterator.next().getObject());
            count++;
        }
        System.out.println("count: " + count);
    }

    @Test
    public void get() {
        System.out.println(queue.get(1L));
        System.out.println(queue.get(1538050452008950L));
    }

    @Test
    public void getAll() {
        List<Long> list = new ArrayList<>(500);
        Iterator<QueueElement<String>> iterator = queue.iterator();
        while (iterator.hasNext()) {
            list.add(iterator.next().getSequence());
        }
        System.out.println("size: " + list.size());
        queue.getAll(list).forEach((k, v) -> System.out.println("k=" + k + ", v=" + v));
    }

    @Test
    public void set() {
        System.out.println(queue.set(1L, "start"));
        System.out.println(queue.set(1537676841962968L, "exist"));
    }

    @Test
    public void remove() {
        System.out.println(queue.remove(1L));
        QueueElement<String> element = queue.peek();
        if (element == null) {
            System.out.println("element is null");
        } else {
            System.out.println(queue.remove(element.getSequence()));
        }
    }

    @Test
    public void removeAll() {
        List<Long> list = new ArrayList<>(500);
        Iterator<QueueElement<String>> iterator = queue.iterator();
        while (iterator.hasNext()) {
            list.add(iterator.next().getSequence());
        }
        queue.removeAll(list);
    }

    @Test
    public void delete() {
        queue.delete();
    }
}
