package com.aliyun.iotx.fluentable;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.aliyun.iotx.fluentable.api.GenericCounter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author jiehong.jh
 * @date 2018/9/23
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = JunitConfiguration.class)
public class TableStoreCounterTest {

    @Autowired
    private FluentableService fluentableService;

    private GenericCounter counter;

    @Before
    public void setUp() {
        counter = fluentableService.opsForCounter("key1");
    }

    @Test
    public void get() {
        System.out.println(counter.get());
    }

    @Test
    public void set() {
        System.out.println(counter.set(3));
    }

    @Test
    public void compareAndSet() {
        System.out.println(counter.compareAndSet(5, 4));
    }

    @Test
    public void getAndIncrement() {
        System.out.println(counter.getAndIncrement());
    }

    @Test
    public void getAndDecrement() {
        System.out.println(counter.getAndDecrement());
    }

    @Test
    public void incrementAndGet() {
        System.out.println(counter.incrementAndGet());
    }

    @Test
    public void decrementAndGet() {
        System.out.println(counter.decrementAndGet());
    }

    @Test
    public void getAndAdd() {
        System.out.println(counter.getAndAdd(2));
    }

    @Test
    public void addAndGet() {
        System.out.println(counter.addAndGet(2));
    }

    @Test
    public void delete() {
        counter.delete();
    }

    @Test
    public void multiIncrement() throws InterruptedException {
        int nThreads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        for (int i = 0; i < nThreads; i++) {
            executor.execute(() -> {
                for (int j = 0; j < 5; j++) {
                    long value = counter.incrementAndGet();
                    System.out.println(Thread.currentThread().getName() + ": " + value);
                }
            });
        }
        TimeUnit.SECONDS.sleep(3);
    }
}
