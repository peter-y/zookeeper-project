package com.ycz.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//CuratorListener
public class CuratorListener1Test {

    private static final Logger logger = LoggerFactory.getLogger(CuratorListener1Test.class);

    public static void main(String[] args) {
        CuratorFramework client = getClient();
        String path = "/curator_li";
        try {
            client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(path, "first".getBytes());
            CuratorListener listener = new CuratorListener() {
                public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
                    logger.info("监听事件 {}", event.toString());
                }
            };
            client.getCuratorListenable().addListener(listener);//注册的时候不触发事件
            client.getData().inBackground().forPath(path);
            client.getData().inBackground().forPath(path);//触发多次
            client.setData().forPath(path, "dada".getBytes());//不触发listener
            client.setData().forPath(path, "dudu".getBytes());
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
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .namespace("test")//这玩意儿相当于是一个root
            .build();
        client.start();
        return client;
    }
}
