package com.ycz.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuratorWatcherTest {

    private static final Logger logger = LoggerFactory.getLogger(CuratorWatcherTest.class);

    private static final String zookeeper_address = "127.0.0.1:2181";
    private static final String zookeeper_curator_path = "/curator_test";

    //有三种观察事件
    public static void main(String[] args) throws Exception {
        CuratorFramework client =
            CuratorFrameworkFactory.newClient(zookeeper_address, new RetryNTimes(10, 5000));
        client.start();
        String data = "hello";
        client.create().creatingParentsIfNeeded().forPath(zookeeper_curator_path, data.getBytes());

        //监听这个路径下的增加删除修改 路径本身的变化并不会触发事件
        PathChildrenCache watcher = new PathChildrenCache(client, zookeeper_curator_path, true);
        watcher.getListenable().addListener(new PathChildrenCacheListener() {
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                ChildData data = event.getData();
                if (data == null) {
                    logger.info("{} no data in event", event);
                } else {
                    logger.info("Receive event: "
                        + "type=[" + event.getType() + "]"
                        + ", path=[" + data.getPath() + "]"
                        + ", data=[" + new String(data.getData()) + "]"
                        + ", stat=[" + data.getStat() + "]");
                }
            }
        });
        watcher.start(StartMode.BUILD_INITIAL_CACHE);
        logger.info("register zk watcher successfully");
        Thread.sleep(Integer.MAX_VALUE);
    }
}
