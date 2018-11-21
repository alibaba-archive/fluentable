package com.aliyun.iotx.fluentable.api;

import com.aliyun.iotx.fluentable.annotation.TableStoreColumn;
import com.aliyun.iotx.fluentable.annotation.TableStorePrimaryKey;
import lombok.Getter;
import lombok.Setter;

/**
 * Support to remove current element
 *
 * @author jiehong.jh
 * @date 2018/9/19
 */
@Getter
@Setter
public class QueueElement<E> {
    @TableStorePrimaryKey
    private String id;
    @TableStorePrimaryKey(order = 1, autoIncrement = true)
    private Long sequence;
    private String type;
    private String value;

    @TableStoreColumn(ignore = true)
    private E object;
}
