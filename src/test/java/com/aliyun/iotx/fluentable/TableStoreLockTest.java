package com.aliyun.iotx.fluentable;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.aliyun.iotx.fluentable.api.GenericLock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author jiehong.jh
 * @date 2018/9/19
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = JunitConfiguration.class)
public class TableStoreLockTest {

    @Autowired
    private FluentableService fluentableService;

    @Test
    public void unlockAndRelease() throws InterruptedException {
        GenericLock lock = fluentableService.opsForLock("lock1");
        if (lock.tryLock(1, TimeUnit.SECONDS)) {
            System.out.println("lock success");
            TimeUnit.MILLISECONDS.sleep(500);
            lock.unlock();
        } else {
            System.out.println("lock failure");
        }
    }

    @Test
    public void unlockAndUnRelease() throws InterruptedException {
        GenericLock lock = fluentableService.opsForLock("lock2");
        if (lock.tryLock(1, TimeUnit.SECONDS)) {
            System.out.println("lock success");
            TimeUnit.MILLISECONDS.sleep(1500);
            lock.unlock();
        } else {
            System.out.println("lock failure");
        }
    }

    @Test
    public void multiTryLock() {
        GenericLock lock = fluentableService.opsForLock("lock3");
        long start = System.currentTimeMillis();
        System.out.println("-->" + lock.tryLock(1, TimeUnit.SECONDS));
        System.out.println(System.currentTimeMillis() - start);
        System.out.println("-->" + lock.tryLock(1, TimeUnit.SECONDS));
        System.out.println(System.currentTimeMillis() - start);
        System.out.println("-->" + lock.tryLock(1, TimeUnit.SECONDS));
        System.out.println(System.currentTimeMillis() - start);
        lock.unlock();
    }

    @Test
    public void multiTyrLockSuccess() throws InterruptedException {
        int nThreads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        for (int i = 0; i < nThreads; i++) {
            executor.execute(() -> {
                GenericLock lock = fluentableService.opsForLock("mlock");
                if (lock.tryLock(1, TimeUnit.SECONDS)) {
                    System.out.println("-->" + Thread.currentThread().getName() + ": success");
                    try {
                        TimeUnit.MILLISECONDS.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    lock.unlock();
                } else {
                    System.out.println("-->" + Thread.currentThread().getName() + ": failure");
                }
            });
        }
        TimeUnit.SECONDS.sleep(1);
    }

    @Test
    public void multiTryLockFailure() throws InterruptedException {
        fluentableService.opsForLock("mlock").tryLock(1, TimeUnit.SECONDS);
        multiTyrLockSuccess();
    }

    @Test
    public void multiLock() throws InterruptedException {
        int nThreads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        final long start = System.currentTimeMillis();
        for (int i = 0; i < nThreads; i++) {
            executor.execute(() -> {
                GenericLock lock = fluentableService.opsForLock("mlock");
                lock.lock(1, TimeUnit.SECONDS);
                try {
                    System.out.println("-->" + Thread.currentThread().getName() + ": " +
                        (System.currentTimeMillis() - start));
                    TimeUnit.MILLISECONDS.sleep(50);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                }
            });
        }
        TimeUnit.SECONDS.sleep(10);
    }

    @Test
    public void multiLockWait() throws InterruptedException {
        int nThreads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        final long start = System.currentTimeMillis();
        for (int i = 0; i < nThreads; i++) {
            executor.execute(() -> {
                GenericLock lock = fluentableService.opsForLock("mlock");
                try {
                    if (lock.tryLock(3, 1, TimeUnit.SECONDS)) {
                        System.out.println("-->" + Thread.currentThread().getName() + " success : " +
                            (System.currentTimeMillis() - start));
                        lock.unlock();
                    } else {
                        System.out.println("-->" + Thread.currentThread().getName() + ": failure");
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }
        TimeUnit.SECONDS.sleep(10);
    }

    @Test
    public void concurrentTryLock() throws InterruptedException {
        GenericLock lock = fluentableService.opsForLock("cclock");
        int nThreads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        for (int i = 0; i < nThreads; i++) {
            executor.execute(() -> {
                if (lock.tryLock(1, TimeUnit.SECONDS)) {
                    System.out.println("-->" + Thread.currentThread().getName() + ": success");
                    try {
                        TimeUnit.MILLISECONDS.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    lock.unlock();
                } else {
                    System.out.println("-->" + Thread.currentThread().getName() + ": failure");
                }
            });
        }
        TimeUnit.SECONDS.sleep(3);
    }

    @Test
    public void concurrentTryLockWait() throws InterruptedException {
        GenericLock lock = fluentableService.opsForLock("cclock");
        int nThreads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        long start = System.currentTimeMillis();
        for (int i = 0; i < nThreads; i++) {
            executor.execute(() -> {
                try {
                    if (lock.tryLock(1, 1, TimeUnit.SECONDS)) {
                        System.out.println("-->" + Thread.currentThread().getName() + ": success, exec: "
                            + (System.currentTimeMillis() - start));
                        try {
                            TimeUnit.MILLISECONDS.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        lock.unlock();
                    } else {
                        System.out.println("-->" + Thread.currentThread().getName() + ": failure");
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }
        TimeUnit.SECONDS.sleep(3);
    }

    @Test
    public void concurrentLock() throws InterruptedException {
        GenericLock lock = fluentableService.opsForLock("cclock");
        int nThreads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        long start = System.currentTimeMillis();
        for (int i = 0; i < nThreads; i++) {
            executor.execute(() -> {
                lock.lock(1, TimeUnit.SECONDS);
                System.out.println("-->" + Thread.currentThread().getName()
                    + " exec: " + (System.currentTimeMillis() - start));
                lock.unlock();
            });
        }
        TimeUnit.SECONDS.sleep(3);
    }
}
