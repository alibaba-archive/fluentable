package com.aliyun.iotx.fluentable;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author jiehong.jh
 * @date 2018/9/14
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "table-store.bridge")
public class BridgeProperties {

    private String list;
    private String listInfo;

    private String lock;

    private String queue;

    private String counter;
}
