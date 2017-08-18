package test;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by HuangQiang on 2016/12/5.
 */
public class Main {
    public static void main(String[] argv) {
        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();
        lock.lock();
        ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
        Lock rlock = rwlock.readLock();
    }
/*
    interface  Builder {
        Procedure2 create(int n);
    }

    public static void test(int n, int m, Builder _2) {
        System.gc();
        long begin = System.nanoTime();
        for(int i = 0 ; i < m ; i++) {
            _2.create(n).call();
        }
        long end = System.nanoTime();
        long cost = end - begin;
        System.out.printf("<%s,%s> cost time:%d average:%d\n", n, m, cost, cost / m);
    }

    public static void once(Builder _2) {
        Trace.info("===============================");
        for(int n : new int[]{0, 3, 10, 50, 100, 1000}) {
            for(int m : new int[]{1000}) {
                test(n, m, _2);
            }
        }
    }

    public static void main(String[] argv) {
        Database.init();
        Trace.info("#################### test Read");
        for(int i = 0 ; i < 3 ; i++) {
            once(n -> new TestRead(n));
        }
        Database.checkpoint();
        Trace.info("#################### test Read");
        for(int i = 0 ; i < 3 ; i++) {
            once(n -> new TestRead(n));
        }
        Database.checkpoint();
        Trace.info("#################### test Write");
        for(int i = 0 ; i < 3 ; i++) {
            once(n -> new TestWrite(n));
        }
        Database.checkpoint();
        Trace.info("#################### test Read Write");
        for(int i = 0 ; i < 3 ; i++) {
            once(n -> new TestReadWrite(n));
        }
        Database.checkpoint();
        Trace.info("#################### test2 Read");
        for(int i = 0 ; i < 3 ; i++) {
            once(n -> new TestRead2(n));
        }
        Database.checkpoint();
        Trace.info("#################### test2 Write");
        for(int i = 0 ; i < 3 ; i++) {
            once(n -> new TestWrite2(n));
        }
        Database.checkpoint();
        Trace.info("#################### test2 Read Write");
        for(int i = 0 ; i < 3 ; i++) {
            once(n -> new TestReadWrite2(n));
        }
        Database.checkpoint();
        Database.close();
    }

    */
}
