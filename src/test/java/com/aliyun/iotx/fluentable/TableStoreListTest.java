package com.aliyun.iotx.fluentable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.aliyun.iotx.fluentable.api.GenericList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author jiehong.jh
 * @date 2018/9/15
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = JunitConfiguration.class)
public class TableStoreListTest {

    @Autowired
    private FluentableService fluentableService;

    private GenericList<String> list;

    @Before
    public void setUp() {
        list = fluentableService.opsForList("key1");
    }

    @Test
    public void size() {
        System.out.println(list.size());
    }

    @Test
    public void isEmpty() {
        System.out.println(list.isEmpty());
    }

    @Test
    public void contains() {
        System.out.println(list.contains("Mick4"));
        System.out.println(list.contains("Mic4"));
    }

    @Test
    public void iterator() {
        for (String s : list) {
            System.out.println("-->" + s);
        }
    }

    @Test
    public void add() {
        list.add("value");
        list.flush();
    }

    @Test
    public void remove() {
        System.out.println(list.remove("value"));
        list.flush();
    }

    @Test
    public void addAll() {
        int size = 6;
        List<String> c = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            c.add("Jack" + i);
        }
        list.addAll(c);
        list.flush();
    }

    @Test
    public void addAllWithIndex() {
        int size = 4;
        List<String> c = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            c.add("Mick" + i);
        }
        list.addAll(0, c);
        list.flush();
    }

    @Test
    public void clear() {
        list.clear();
        list.flush();
    }

    @Test
    public void get() {
        System.out.println(list.get(0));
    }

    @Test
    public void set() {
        list.set(6, "SuperMick");
        list.flush();
    }

    @Test
    public void addWithIndex() {
        list.add(7, "Ele");
        list.add(8, "Chn");
        list.flush();
    }

    @Test
    public void removeWithIndex() {
        list.remove(3);
        list.flush();
    }

    @Test
    public void indexOf() {
        System.out.println(list.indexOf("Jack1"));
        System.out.println(list.indexOf("Mic"));
    }

    @Test
    public void lastIndexOf() {
        System.out.println(list.lastIndexOf("Jack1"));
        System.out.println(list.lastIndexOf("Mic"));
    }

    @Test
    public void delete() {
        list.delete();
    }

    @Test
    public void expire() {
        list.expire(10, TimeUnit.SECONDS);
        list.flush();
    }
}
