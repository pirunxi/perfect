package test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by HuangQiang on 2017/4/28.
 */
public class TestStack {
    public static void main(String[] argv) {
        /*
        Stack<Integer> ss = new Stack<>();
        ss.push(1);
        ss.push(2);
        for(int x : ss) {
            System.out.println(x);
        }


        ReadWriteLock rwlock = new ReentrantReadWriteLock();
        rwlock.readLock().lock();
        System.out.println("read lock");
        rwlock.writeLock().lock();
        System.out.println("write lock");
        rwlock.readLock().unlock();
        rwlock.writeLock().unlock();
        System.out.println("lock all");
         */
        List<Integer> list = new ArrayList<>();
        for(int i = 0 ; i < 10 ; i++)
            list.add(i);
        System.out.println(list);
        list.subList(2, 4).clear();
        System.out.println(list);
    }
}
