package com.aliyun.iotx.fluentable;

import java.util.Comparator;
import java.util.List;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.CreateTableRequest;
import com.alicloud.openservices.tablestore.model.GetRowRequest;
import com.alicloud.openservices.tablestore.model.GetRowResponse;
import com.alicloud.openservices.tablestore.model.PrimaryKeyOption;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.PutRowResponse;
import com.alicloud.openservices.tablestore.model.ReturnType;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.alicloud.openservices.tablestore.model.SingleRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.TableOptions;
import com.alicloud.openservices.tablestore.model.UpdateRowRequest;
import com.alicloud.openservices.tablestore.model.UpdateRowResponse;
import com.aliyun.iotx.fluentable.annotation.TableStoreAnnotationParser;
import com.aliyun.iotx.fluentable.parser.ClassInfo;
import com.aliyun.iotx.fluentable.parser.FieldInfo;
import com.aliyun.iotx.fluentable.util.TableStorePkBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author jiehong.jh
 * @date 2018/9/19
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = JunitConfiguration.class)
public class AutoIncrementTest {

    private static final String TABLE_NAME = "auto";
    @Autowired
    private TableStoreOperations tableStoreTemplate;
    @Autowired
    private TableStoreAnnotationParser annotationParser;
    @Autowired
    private ClassInfo classInfo;
    @Autowired
    private SyncClientInterface syncClient;

    //@Test
    public void createTable() {
        TableMeta tableMeta = new TableMeta(TABLE_NAME);
        List<FieldInfo> infos = classInfo.getFields(AutoIncrement.class);
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
    public void insert() {
        AutoIncrement increment = new AutoIncrement();
        increment.setId("hi");
        increment.setJson("lora-want");
        RowPutChange rowPutChange = tableStoreTemplate.put(TABLE_NAME)
            // rowNotExist 会失败
            .with(annotationParser.parse(increment)).rowIgnore().rowChange();
        rowPutChange.setReturnType(ReturnType.RT_PK);
        PutRowResponse response = syncClient.putRow(new PutRowRequest(rowPutChange));
        Row returnRow = response.getRow();
        if (returnRow != null) {
            System.out.println("-->PrimaryKey:" + returnRow.getPrimaryKey().toString());
        }
    }

    @Test
    public void insertWithIndex() {
        AutoIncrement increment = new AutoIncrement();
        increment.setId("hi");
        // 主键必须存在，否则异常
        increment.setIndex(1537353465082000L);
        increment.setJson("lora-wanxxx");
        RowPutChange rowPutChange = tableStoreTemplate.put(TABLE_NAME)
            .with(annotationParser.parse(increment, false)).rowIgnore().rowChange();
        rowPutChange.setReturnType(ReturnType.RT_PK);
        PutRowResponse response = syncClient.putRow(new PutRowRequest(rowPutChange));
        Row returnRow = response.getRow();
        if (returnRow != null) {
            System.out.println("-->PrimaryKey:" + returnRow.getPrimaryKey().toString());
        }
    }

    @Test
    public void update() {
        AutoIncrement increment = new AutoIncrement();
        increment.setId("hi");
        increment.setJson("lora-link");
        // 如果自增，数据不存在等价于Put
        RowUpdateChange rowUpdateChange = tableStoreTemplate.update(TABLE_NAME)
            .with(annotationParser.parse(increment)).rowIgnore().rowChange();
        rowUpdateChange.setReturnType(ReturnType.RT_PK);
        UpdateRowResponse response = syncClient.updateRow(new UpdateRowRequest(rowUpdateChange));
        Row returnRow = response.getRow();
        if (returnRow != null) {
            System.out.println("-->PrimaryKey:" + returnRow.getPrimaryKey().toString());
        }
    }

    @Test
    public void updateWithIndex() {
        AutoIncrement increment = new AutoIncrement();
        increment.setId("hi");
        // 主键必须存在，否则异常
        increment.setIndex(1537360159396000L);
        increment.setJson("lora-link-indexs");
        RowUpdateChange rowUpdateChange = tableStoreTemplate.update(TABLE_NAME)
            .with(annotationParser.parse(increment, false)).rowIgnore().rowChange();
        rowUpdateChange.setReturnType(ReturnType.RT_PK);
        UpdateRowResponse response = syncClient.updateRow(new UpdateRowRequest(rowUpdateChange));
        Row returnRow = response.getRow();
        if (returnRow != null) {
            System.out.println("-->PrimaryKey:" + returnRow.getPrimaryKey().toString());
        }
    }

    @Test
    public void select() {
        SingleRowQueryCriteria criteria = tableStoreTemplate.select().from(TABLE_NAME).where()
            .pkEqual(new TableStorePkBuilder().add("id", "hi")
                .add("index", 1537360159396000L).build()).rowQuery();
        GetRowResponse response = syncClient.getRow(new GetRowRequest(criteria));
        Row row = response.getRow();
        if (row != null) {
            AutoIncrement increment = new AutoIncrement();
            annotationParser.parseInto(row, increment);
            System.out.println(increment);
        } else {
            System.out.println("row is null");
        }
    }
}
