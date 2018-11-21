package com.aliyun.iotx.fluentable;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.aliyun.iotx.fluentable.api.JsonHelper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Json serializer
 *
 * @author jiehong.jh
 * @date 2018/9/20
 */
public class JackJsonHelper implements JsonHelper {

    private final Map<String, Class<?>> types = new ConcurrentHashMap<>();
    private ObjectMapper objectMapper;

    public JackJsonHelper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String toJson(Object o) {
        try {
            return objectMapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    @Override
    public <E> E fromJson(String json, Class<E> type) {
        try {
            return objectMapper.readValue(json, type);
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public <E> E fromJson(String json, String typeName) {
        return fromJson(json, getType(typeName));
    }

    @SuppressWarnings("unchecked")
    private <E> Class<E> getType(String typeName) {
        Class<E> type = (Class<E>)types.get(typeName);
        if (null == type) {
            try {
                type = (Class<E>)Class.forName(typeName);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
            types.put(typeName, type);
        }
        return type;
    }
}
