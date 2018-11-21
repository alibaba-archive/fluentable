package com.aliyun.iotx.fluentable.parser;

import com.alicloud.openservices.tablestore.model.Row;

/**
 * @author jiehong.jh
 * @date 2018/8/2
 */
public interface RowParser {

    /**
     * parse TableStore row column list order by timestamp desc
     *
     * @param row
     * @param fieldInfo
     * @return field value list or empty
     */
    FieldData parse(Row row, FieldInfo fieldInfo);

    /**
     * parse TableStore row column list order by timestamp desc
     *
     * @param row
     * @param fieldInfo
     * @return field value list or empty
     */
    FieldData parseLatest(Row row, FieldInfo fieldInfo);
}
