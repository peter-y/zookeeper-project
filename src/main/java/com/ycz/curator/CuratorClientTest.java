package com.ycz.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuratorClientTest {

    private static final Logger logger = LoggerFactory.getLogger(CuratorClientTest.class);

    private static final String zookeeper_address = "127.0.0.1:2181";
    private static final String zookeeper_curator_path = "/curator_test";

    private static void print(String... cmds) {
        StringBuilder text = new StringBuilder("$ ");
        for (String cmd : cmds) {
            text.append(cmd).append(" ");
        }
        logger.info(text.toString());
    }

    private static void print(Object result) {
        logger.info("{}",
            result instanceof byte[]
                ? new String((byte[]) result)
                : result);
    }

    public static void main(String[] args) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeper_address, new RetryNTimes(10, 5000));
        client.start();
        logger.info("start successfully");
        String data1 = "root";
        //这个不是 按需创建吗.... 有重复的时候就报错了
        print("create", zookeeper_curator_path, data1);
        client.create().creatingParentsIfNeeded().forPath(zookeeper_curator_path, data1.getBytes());
        print("ls", "/");
        print(client.getChildren().forPath("/"));
        print("get", zookeeper_curator_path);
        print(client.getData().forPath(zookeeper_curator_path));
        String data2 = "world";
        print("set", zookeeper_curator_path, data2);
        client.setData().forPath(zookeeper_curator_path, data2.getBytes());
        print("get", zookeeper_curator_path);
        print(client.getData().forPath(zookeeper_curator_path));
        print("delete", zookeeper_curator_path);
        client.delete().forPath(zookeeper_curator_path);
        print("ls", "/");
        print(client.getChildren().forPath("/"));
        client.close();
    }
}
