package com.ycz.curator;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuratorLockOrderNoTest {

    private static final Logger logger = LoggerFactory.getLogger(CuratorLockOrderNoTest.class);

    public static void main(String[] args) {
        CuratorFramework client = getClient();
        String path = "/lockp";
        final InterProcessMutex lock = new InterProcessMutex(client, path);
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        final long startTime = new Date().getTime();
        for (int i = 0; i < 10; i++) {
            new Thread(new Runnable() {

                public void run() {
                    try {
                        countDownLatch.await();
                        lock.acquire();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss|SSS");
                    logger.info(sdf.format(new Date()));

                    try {
                        lock.release();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    logger.info("显示此线程大概花费时间（等待+执行）:" + (new Date().getTime() - startTime) + "ms");
                }
            }).start();
        }
        logger.info("创建线程花费时间:" + (new Date().getTime() - startTime) + "ms");
        countDownLatch.countDown();
    }

    private static CuratorFramework getClient() {
        CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString("127.0.0.1:2181")
            .retryPolicy(new ExponentialBackoffRetry(1000,3))
            .sessionTimeoutMs(6000)
            .connectionTimeoutMs(4000)
            .namespace("test")
            .build();
        client.start();
        return client;
    }
}
