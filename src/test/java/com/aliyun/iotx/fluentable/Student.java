package com.aliyun.iotx.fluentable;

import java.util.Date;

import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.aliyun.iotx.fluentable.annotation.TableStoreColumn;
import com.aliyun.iotx.fluentable.annotation.TableStorePrimaryKey;
import lombok.Getter;
import lombok.Setter;

/**
 * @author jiehong.jh
 * @date 2018/9/7
 */
@Getter
@Setter
public class Student {

    @TableStorePrimaryKey(type = PrimaryKeyType.INTEGER, order = 2)
    private Integer grade;
    @TableStorePrimaryKey(order = 1)
    private String school;
    @TableStorePrimaryKey(order = 3)
    private String name;

    /**
     * 性别
     */
    private boolean sex;
    /**
     * 爱好
     */
    private String hobby;

    @TableStoreColumn(type = ColumnType.INTEGER)
    private Date birth;

    /**
     * 身高
     */
    private float height;

    @Override
    public String toString() {
        return "Student{" +
            "grade=" + grade +
            ", school='" + school + '\'' +
            ", name='" + name + '\'' +
            ", sex=" + sex +
            ", hobby='" + hobby + '\'' +
            ", birth=" + birth +
            ", height=" + height +
            '}';
    }
}
