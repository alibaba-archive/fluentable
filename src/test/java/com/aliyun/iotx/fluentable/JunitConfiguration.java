package com.aliyun.iotx.fluentable;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;

/**
 * @author jiehong.jh
 * @date 2018/9/7
 */
@Configuration
@PropertySource("classpath:table-store-dev.properties")
@Import(FluentableAutoConfiguration.class)
public class JunitConfiguration {

    @Value("${endpoint}")
    private String endpoint;
    @Value("${instance}")
    private String instance;
    @Value("${accessKey}")
    private String accessKey;
    @Value("${secretKey}")
    private String secretKey;

    @Bean
    public SyncClientInterface syncClient() {
        return new SyncClient(endpoint, accessKey, secretKey, instance);
    }
}
