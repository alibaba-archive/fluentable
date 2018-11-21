package com.aliyun.iotx.fluentable.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.aliyun.iotx.fluentable.converter.DefaultConverter;
import com.aliyun.iotx.fluentable.converter.TableStoreConverter;
import com.aliyun.iotx.fluentable.formatter.DefaultFormatter;
import com.aliyun.iotx.fluentable.formatter.TableStoreFormatter;

/**
 * @author jiehong.jh
 * @date 2018/8/2
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TableStorePrimaryKey {

    /**
     * @return 列名
     */
    String value() default "";

    /**
     * @return 主键是否自增，如果为true，那么类型默认为PrimaryKeyType.INTEGER
     */
    boolean autoIncrement() default false;

    /**
     * @return 是否忽略该字段作为TableStore列
     */
    boolean ignore() default false;

    /**
     * @return 主键列的顺序（从小到大）
     */
    int order() default 0;

    /**
     * @return 列类型
     */
    PrimaryKeyType type() default PrimaryKeyType.STRING;

    /**
     * @return TableStore类型转换为Java类型
     */
    Class<? extends TableStoreConverter> converter() default DefaultConverter.class;

    /**
     * @return Java类型转换为TableStore类型
     */
    Class<? extends TableStoreFormatter> formatter() default DefaultFormatter.class;
}
