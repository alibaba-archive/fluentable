package com.aliyun.iotx.fluentable.util;

import java.util.Optional;
import java.util.function.Consumer;

import com.alicloud.openservices.tablestore.model.Row;

/**
 * TableStore row extractor,return the value of newest version.
 *
 * @author jiehong.jh
 * @date 2018/6/4
 */
public class TableStoreRowExtractor {

    private Row row;

    public TableStoreRowExtractor(Row row) {
        this.row = row;
    }

    public TableStoreRowExtractor pkAsString(String name, Consumer<String> valueConsumer) {
        valueConsumer.accept(row.getPrimaryKey().getPrimaryKeyColumn(name).getValue().asString());
        return this;
    }

    public TableStoreRowExtractor pkAsLong(String name, Consumer<Long> valueConsumer) {
        valueConsumer.accept(row.getPrimaryKey().getPrimaryKeyColumn(name).getValue().asLong());
        return this;
    }

    public TableStoreRowExtractor pkAsBinary(String name, Consumer<byte[]> valueConsumer) {
        valueConsumer.accept(row.getPrimaryKey().getPrimaryKeyColumn(name).getValue().asBinary());
        return this;
    }

    public TableStoreRowExtractor asString(String name, Consumer<String> valueConsumer) {
        Optional.ofNullable(row.getLatestColumn(name))
            .map(column -> column.getValue().asString())
            .ifPresent(valueConsumer);
        return this;
    }

    public TableStoreRowExtractor asLong(String name, Consumer<Long> valueConsumer) {
        Optional.ofNullable(row.getLatestColumn(name))
            .map(column -> column.getValue().asLong())
            .ifPresent(valueConsumer);
        return this;
    }

    public TableStoreRowExtractor asDouble(String name, Consumer<Double> valueConsumer) {
        Optional.ofNullable(row.getLatestColumn(name))
            .map(column -> column.getValue().asDouble())
            .ifPresent(valueConsumer);
        return this;
    }

    public TableStoreRowExtractor asBoolean(String name, Consumer<Boolean> valueConsumer) {
        Optional.ofNullable(row.getLatestColumn(name))
            .map(column -> column.getValue().asBoolean())
            .ifPresent(valueConsumer);
        return this;
    }

    public TableStoreRowExtractor asBinary(String name, Consumer<byte[]> valueConsumer) {
        Optional.ofNullable(row.getLatestColumn(name))
            .map(column -> column.getValue().asBinary())
            .ifPresent(valueConsumer);
        return this;
    }

}
