package com.aliyun.iotx.fluentable.api;

/**
 * @author jiehong.jh
 * @date 2018/9/23
 */
public interface GenericCounter {

    /**
     * Gets the current value.
     *
     * @return the current value
     */
    long get();

    /**
     * Sets to the given value.
     *
     * @param newValue the new value
     */
    boolean set(long newValue);

    /**
     * Atomically sets the value to the given updated value if the current value {@code ==} the expected value.
     *
     * @param expect the expected value
     * @param update the new value
     * @return {@code true} if successful. False return indicates that the actual value was not equal to the expected
     * value.
     */
    boolean compareAndSet(long expect, long update);

    /**
     * Atomically increments by one the current value.
     *
     * @return the previous value
     */
    long getAndIncrement();

    /**
     * Atomically decrements by one the current value.
     *
     * @return the previous value
     */
    long getAndDecrement();

    /**
     * Atomically increments by one the current value.
     *
     * @return the updated value
     */
    long incrementAndGet();

    /**
     * Atomically decrements by one the current value.
     *
     * @return the updated value
     */
    long decrementAndGet();

    /**
     * Atomically adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the previous value
     */
    long getAndAdd(int delta);

    /**
     * Atomically adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the updated value
     */
    long addAndGet(int delta);

    /**
     * Delete counter
     */
    void delete();
}
