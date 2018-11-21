package com.aliyun.iotx.fluentable;

import com.aliyun.iotx.fluentable.annotation.TableStorePrimaryKey;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author jiehong.jh
 * @date 2018/9/19
 */
@Getter
@Setter
@ToString
public class AutoIncrement {
    @TableStorePrimaryKey
    private String id;
    @TableStorePrimaryKey(order = 1, autoIncrement = true)
    private Long index;
    private String json;
}
