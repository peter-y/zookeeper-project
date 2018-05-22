package com.ycz.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//Watcher
public class CuratorListenerWatcherTest {

    private static final Logger logger = LoggerFactory.getLogger(CuratorListenerWatcherTest.class);

    public static void main(String[] args) {
        CuratorFramework client = getClient();
        client.start();
        String path = "/curator_li";
        try {
            client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(path);
            byte[] content = client.getData().usingWatcher(new Watcher() {
                public void process(WatchedEvent event) {
                    logger.info("监听器 {}", event.toString());
                }
            }).forPath(path);//都是一次性监听
            logger.info("内容是 {}", new String(content));
            client.setData().forPath(path, "hahah".getBytes());
            client.setData().forPath(path, "heihei".getBytes());
            Thread.sleep(10000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static CuratorFramework getClient() {
        CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString("127.0.0.1:2181")
            .connectionTimeoutMs(5000)
            .sessionTimeoutMs(5000)
            .retryPolicy(new RetryNTimes(3, 1000))
            .namespace("test")//这玩意儿相当于是一个root
            .build();
        return client;
    }
}
