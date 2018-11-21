package com.aliyun.iotx.fluentable.api;

/**
 * Flush Collision
 *
 * @author jiehong.jh
 * @date 2018/9/13
 */
public class FlushCollisionException extends RuntimeException {

    public FlushCollisionException() {
        super();
    }

    public FlushCollisionException(String message) {
        super(message);
    }

    public FlushCollisionException(String message, Throwable cause) {
        super(message, cause);
    }

    public FlushCollisionException(Throwable cause) {
        super(cause);
    }
}
