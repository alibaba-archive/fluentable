package com.aliyun.iotx.fluentable.api;

/**
 * @author jiehong.jh
 * @date 2018/9/20
 */
public interface JsonHelper {

    /**
     * Method that can be used to serialize any Java value as a String.
     *
     * @param o
     * @return
     */
    String toJson(Object o);

    /**
     * Method to deserialize JSON content from given JSON content String.
     *
     * @param json
     * @param type
     * @param <E>
     * @return
     */
    <E> E fromJson(String json, Class<E> type);

    /**
     * Method to deserialize JSON content from given JSON content String.
     *
     * @param json
     * @param typeName
     * @param <E>
     * @return
     */
    <E> E fromJson(String json, String typeName);
}
