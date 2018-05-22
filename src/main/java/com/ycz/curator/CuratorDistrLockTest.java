package com.ycz.curator;

import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuratorDistrLockTest {

    private static final Logger logger = LoggerFactory.getLogger(CuratorDistrLockTest.class);

    private static final String zookeeper_address = "127.0.0.1:2181";
    private static final String zookeeper_curator_path = "/curator_lock";

    public static void main(String[] args) {
        final CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeper_address, new RetryNTimes(10, 5000));
        client.start();
        Thread t1 = new Thread(new Runnable() {
            public void run() {
                doWithLock(client);
            }
        }, "t1");

        Thread t2 = new Thread(new Runnable() {
            public void run() {
                doWithLock(client);
            }
        }, "t2");
        t1.start();
        t2.start();
    }

    private static void doWithLock(CuratorFramework client) {
        //互斥锁 path 会被直接创建
        InterProcessMutex lock = new InterProcessMutex(client, zookeeper_curator_path);
        try {
            Long time = TimeUnit.SECONDS.toMillis(10 * 1000);//1万秒转换为毫秒
            logger.info("time {}", time);
            if (lock.acquire(10 * 1000, TimeUnit.SECONDS)) {
                logger.info("{} hold lock", Thread.currentThread().getName());
                Thread.sleep(5000);
                logger.info("{} release lock", Thread.currentThread().getName());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                lock.release();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
