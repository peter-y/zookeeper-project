package com.ycz.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//NodeCache
public class CuratorListenerNodeCacheTest {

    private static final Logger logger = LoggerFactory.getLogger(CuratorListenerNodeCacheTest.class);

    public static void main(String[] args) {
        CuratorFramework client = getClient();
        String path = "/curator_li";
        try {
            client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(path);
            final NodeCache cache = new NodeCache(client, path);
            cache.start();
            cache.getListenable().addListener(new NodeCacheListener() {
                public void nodeChanged() throws Exception {
                    logger.info("监听事件触发");
                    logger.info("重新获得节点内容为 {}",new String(cache.getCurrentData().getData()));
                }
            });
            //只是因为curator 会帮助重新注册监听，但是注册监听也是需要时间，连续的数据变化可能会造成
            //在没有成功注册上事件之前，数据就产生了新的变化，所以会丢失监听
            //client的断连也会造成事件的丢失
            client.setData().forPath(path,"456".getBytes());
            client.setData().forPath(path,"789".getBytes());
            client.setData().forPath(path,"123".getBytes());
            client.setData().forPath(path,"222".getBytes());
            client.setData().forPath(path,"333".getBytes());
            client.setData().forPath(path,"444".getBytes());
            //会事件丢失
            Thread.sleep(100000);
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
        client.start();
        return client;
    }
}
