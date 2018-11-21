package com.aliyun.iotx.fluentable.model;

/**
 * @author jiehong.jh
 * @date 2018/9/7
 */
public class OptionalColumn {
    private String name;
    private OptionalColumnValue value;
    private Long ts;

    public OptionalColumn(String name, OptionalColumnValue value) {
        this.name = name;
        this.value = value;
    }

    public OptionalColumn(String name, OptionalColumnValue value, Long ts) {
        this.name = name;
        this.value = value;
        this.ts = ts;
    }

    public String getName() {
        return name;
    }

    public OptionalColumnValue getValue() {
        return value;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }
}
