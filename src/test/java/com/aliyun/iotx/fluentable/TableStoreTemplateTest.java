package com.aliyun.iotx.fluentable;

import java.util.Date;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.DeleteRowRequest;
import com.alicloud.openservices.tablestore.model.GetRowRequest;
import com.alicloud.openservices.tablestore.model.GetRowResponse;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.RowDeleteChange;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.alicloud.openservices.tablestore.model.SingleRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.UpdateRowRequest;
import com.aliyun.iotx.fluentable.annotation.TableStoreAnnotationParser;
import com.aliyun.iotx.fluentable.util.TableStorePkBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author jiehong.jh
 * @date 2018/9/7
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = JunitConfiguration.class)
public class TableStoreTemplateTest {

    @Autowired
    private SyncClientInterface syncClient;
    @Autowired
    private TableStoreOperations tableStoreTemplate;
    @Autowired
    private TableStoreAnnotationParser annotationParser;

    @Test
    public void select() {
        SingleRowQueryCriteria rowQuery = tableStoreTemplate.select().from("student").where()
            .pkEqual(new TableStorePkBuilder()
                .add("school", "铃兰男子高中")
                .add("grade", 3)
                .add("name", "泷谷源治")
                .build()).rowQuery();
        GetRowResponse response = syncClient.getRow(new GetRowRequest(rowQuery));
        Student student = annotationParser.parse(response.getRow(), Student.class).getObject();
        System.out.println(student);
    }

    @Test
    public void insert() {
        Student student = new Student();
        student.setSchool("铃兰男子高中");
        student.setGrade(3);
        student.setName("泷谷源治");
        student.setSex(true);
        student.setHobby("制霸铃兰");
        student.setBirth(new Date());
        student.setHeight(1.84f);
        RowPutChange rowChange = tableStoreTemplate.put("student")
            .with(annotationParser.parse(student))
            .rowNotExist().rowChange();
        syncClient.putRow(new PutRowRequest(rowChange));
    }

    @Test
    public void update() {
        SingleRowQueryCriteria rowQuery = tableStoreTemplate.select().from("student").where()
            .pkEqual(new TableStorePkBuilder()
                .add("school", "铃兰男子高中")
                .add("grade", 3)
                .add("name", "泷谷源治")
                .build()).rowQuery();
        GetRowResponse response = syncClient.getRow(new GetRowRequest(rowQuery));
        Student student = annotationParser.parse(response.getRow(), Student.class).getObject();
        student.setHobby(null);
        student.setHeight(1.90f);
        RowUpdateChange rowChange = tableStoreTemplate.update("student")
            .with(annotationParser.parse(student))
            .rowExist().rowChange();
        syncClient.updateRow(new UpdateRowRequest(rowChange));
    }

    @Test
    public void delete() {
        RowDeleteChange rowChange = tableStoreTemplate.delete("student")
            .where(new TableStorePkBuilder()
                .add("school", "铃兰男子高中")
                .add("grade", 3)
                .add("name", "泷谷源治")
                .build()).rowExist().rowChange();
        syncClient.deleteRow(new DeleteRowRequest(rowChange));
    }
}
