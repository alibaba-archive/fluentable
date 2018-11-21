package com.aliyun.iotx.fluentable;

import java.util.Iterator;

import com.alicloud.openservices.tablestore.model.Row;
import com.aliyun.iotx.fluentable.annotation.TableStoreAnnotationParser;

/**
 * @author jiehong.jh
 * @date 2018/9/20
 */
public class TableStoreIterator<T> implements Iterator<T> {

    private Class<T> clazz;
    private Iterator<Row> iterator;

    private TableStoreAnnotationParser annotationParser;

    public TableStoreIterator(Class<T> clazz, Iterator<Row> iterator,
                              TableStoreAnnotationParser annotationParser) {
        this.clazz = clazz;
        this.iterator = iterator;
        this.annotationParser = annotationParser;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public T next() {
        return annotationParser.parseLatest(iterator.next(), clazz);
    }

    @Override
    public void remove() {
        iterator.remove();
    }
}
