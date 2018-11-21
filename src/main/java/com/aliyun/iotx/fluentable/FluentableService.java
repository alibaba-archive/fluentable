package com.aliyun.iotx.fluentable;

import java.util.List;
import java.util.Map;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.aliyun.iotx.fluentable.api.GenericCounter;
import com.aliyun.iotx.fluentable.api.GenericList;
import com.aliyun.iotx.fluentable.api.GenericLock;
import com.aliyun.iotx.fluentable.api.GenericQueue;

/**
 * @author jiehong.jh
 * @date 2018/9/25
 */
public interface FluentableService {

    /**
     * 获取TableStore原生的Client
     *
     * @return
     */
    SyncClientInterface syncClient();

    /**
     * 获取TableStore的计数器
     *
     * @param key
     * @return
     */
    GenericCounter opsForCounter(String key);

    /**
     * Returns lock instance by name.
     * <p>
     * Implements a <b>non-fair</b> locking so doesn't guarantees an acquire order by threads.
     *
     * @param key - key of object
     * @return Lock object
     */
    GenericLock opsForLock(String key);

    /**
     * 获取TableStore的先进先出队列
     *
     * @param key
     * @param <T>
     * @return
     */
    <T> GenericQueue<T> opsForQueue(String key);

    /**
     * 获取TableStore的列表
     *
     * @param key
     * @param <T>
     * @return
     */
    <T> GenericList<T> opsForList(String key);

    /**
     * 批量获取TableStore的列表
     *
     * @param keys 限制大小为100
     * @param <T>
     * @return key - GenericList
     */
    <T> Map<String, GenericList<T>> opsForMultiList(List<String> keys);
}
