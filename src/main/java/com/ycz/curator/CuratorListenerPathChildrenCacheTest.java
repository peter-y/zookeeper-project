package com.ycz.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//Watcher
public class CuratorListenerPathChildrenCacheTest {

    private static final Logger logger = LoggerFactory.getLogger(CuratorListenerPathChildrenCacheTest.class);

    public static void main(String[] args) throws Exception {
        CuratorFramework client = getClient();
        String parentPath = "/p1";

        PathChildrenCache pathChildrenCache = new PathChildrenCache(client,parentPath,true);
        pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {

            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                System.out.println("事件类型："  + event.getType() + "；操作节点：" + event.getData().getPath());
            }
        });

        String path = "/p1/c1";
        client.create().withMode(CreateMode.PERSISTENT).forPath(path);
        Thread.sleep(1000); // 此处需留意，如果没有现成睡眠则无法触发监听事件
        client.delete().forPath(path);

        Thread.sleep(15000);
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
