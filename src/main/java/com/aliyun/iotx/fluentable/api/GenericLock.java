package com.aliyun.iotx.fluentable.api;

import java.util.concurrent.TimeUnit;

/**
 * 建议降级用的 TableStoreLock
 *
 * @author jiehong.jh
 * @date 2018/9/19
 */
public interface GenericLock {

    /**
     * Returns <code>true</code> as soon as the lock is acquired.
     * If the lock is currently held by another thread in this or any
     * other process in the distributed system this method keeps trying
     * to acquire the lock for up to <code>waitTime</code> before
     * giving up and returning <code>false</code>. If the lock is acquired,
     * it is held until <code>unlock</code> is invoked, or until <code>leaseTime</code>
     * have passed since the lock was granted - whichever comes first.
     *
     * @param leaseTime lease time
     * @param unit time unit
     * @return <code>true</code> if lock has been successfully acquired
     */
    boolean tryLock(long leaseTime, TimeUnit unit);

    /**
     * Returns <code>true</code> as soon as the lock is acquired.
     * If the lock is currently held by another thread in this or any
     * other process in the distributed system this method keeps trying
     * to acquire the lock for up to <code>waitTime</code> before
     * giving up and returning <code>false</code>. If the lock is acquired,
     * it is held until <code>unlock</code> is invoked, or until <code>leaseTime</code>
     * have passed since the lock was granted - whichever comes first.
     *
     * @param waitTime the maximum time to aquire the lock
     * @param leaseTime lease time
     * @param unit time unit
     * @return <code>true</code> if lock has been successfully acquired
     * @throws InterruptedException if the current thread is
     *         interrupted while acquiring the lock (and interruption
     *         of lock acquisition is supported)
     */
    boolean tryLock(long waitTime, long leaseTime, TimeUnit unit) throws InterruptedException;

    /**
     * Acquires the lock.
     *
     * <p>If the lock is not available then the current thread becomes
     * disabled for thread scheduling purposes and lies dormant until the
     * lock has been acquired.
     *
     * If the lock is acquired, it is held until <code>unlock</code> is invoked,
     * or until leaseTime milliseconds have passed
     * since the lock was granted - whichever comes first.
     *
     * @param leaseTime the maximum time to hold the lock after granting it,
     *        before automatically releasing it if it hasn't already been released by invoking <code>unlock</code>.
     *        If leaseTime is -1, hold the lock until explicitly unlocked.
     * @param unit the time unit of the {@code leaseTime} argument
     *
     */
    void lock(long leaseTime, TimeUnit unit);

    /**
     * Releases the lock.
     *
     * <p><b>Implementation Considerations</b>
     *
     * <p>A {@code Lock} implementation will usually impose
     * restrictions on which thread can release a lock (typically only the
     * holder of the lock can release it) and may throw
     * an (unchecked) exception if the restriction is violated.
     * Any restrictions and the exception
     * type must be documented by that {@code Lock} implementation.
     */
    void unlock();
}
