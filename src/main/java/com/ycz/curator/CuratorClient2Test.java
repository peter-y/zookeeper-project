package com.ycz.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuratorClient2Test {

    private static final Logger logger = LoggerFactory.getLogger(CuratorClient2Test.class);

    private static final String zookeeper_address = "127.0.0.1:2181";
    private static final String zookeeper_curator_path = "/curator_test/t1";

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
        //这个不是 按需创建吗.... 有重复的时候就报错了 创建固定的持久节点的时候如果存在还是会报错
        print("create", zookeeper_curator_path, data1);
        //按需创建父节点 适用在 /持久节点/临时节点 这种情况
        client.create().creatingParentsIfNeeded()
            .withMode(CreateMode.EPHEMERAL)
            .forPath(zookeeper_curator_path, data1.getBytes());
        print("ls", "/");
        print(client.getChildren().forPath("/"));
        client.create().creatingParentsIfNeeded()
            .forPath("/test/delete/d1");//创建多级节点
        client.delete().deletingChildrenIfNeeded().forPath("/test/delete/d1");//删除子节点 父节点仍然存在，节点下如果还有子节点，也会删除
        client.delete().withVersion(10).forPath("/test/delete");//带版本的删除
        client.delete().guaranteed().forPath("/test/delete");//强删，一直重试直到删成功了位置
        client.close();
    }
}
