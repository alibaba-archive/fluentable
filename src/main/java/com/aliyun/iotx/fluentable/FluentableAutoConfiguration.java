package com.aliyun.iotx.fluentable;

import java.util.List;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.aliyun.iotx.fluentable.annotation.TableStoreAnnotationParser;
import com.aliyun.iotx.fluentable.api.JsonHelper;
import com.aliyun.iotx.fluentable.converter.DefaultConverter;
import com.aliyun.iotx.fluentable.converter.TableStoreConverter;
import com.aliyun.iotx.fluentable.formatter.DefaultFormatter;
import com.aliyun.iotx.fluentable.formatter.TableStoreFormatter;
import com.aliyun.iotx.fluentable.parser.ClassInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author jiehong.jh
 * @date 2018/8/6
 */
@EnableConfigurationProperties(BridgeProperties.class)
@Configuration
public class FluentableAutoConfiguration {

    @Bean
    public TableStoreConverter defaultConverter() {
        return new DefaultConverter();
    }

    @Bean
    public TableStoreFormatter defaultFormatter() {
        return new DefaultFormatter();
    }

    @Bean
    public ClassInfo classInfoCache(List<TableStoreConverter> converters, List<TableStoreFormatter> formatters) {
        return new ClassInfo(converters, formatters);
    }

    @Bean
    public TableStoreAnnotationParser annotationParser(ClassInfo classInfo) {
        return new TableStoreAnnotationParser(classInfo);
    }

    @Bean
    public TableStoreOperations tableStoreTemplate() {
        return new TableStoreTemplate();
    }

    @Configuration
    @ConditionalOnClass(ObjectMapper.class)
    static class EnhanceAutoConfiguration {

        @Bean
        @ConditionalOnMissingBean
        public ObjectMapper objectMapper() {
            return new ObjectMapper();
        }

        @Bean
        @ConditionalOnMissingBean
        public JsonHelper jsonHelper(ObjectMapper objectMapper) {
            return new JackJsonHelper(objectMapper);
        }

        @Bean
        @ConditionalOnMissingBean
        public FluentableService fluentableService(SyncClientInterface syncClient) {
            return new TableStoreEnhancer(syncClient);
        }
    }
}
