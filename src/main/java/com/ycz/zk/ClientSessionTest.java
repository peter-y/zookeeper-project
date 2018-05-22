package com.ycz.zk;

import java.util.Date;
import org.apache.curator.utils.DefaultZookeeperFactory;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientSessionTest {

    private static final Logger logger = LoggerFactory.getLogger(ClientSessionTest.class);


    public static void main(String[] args) {
        long startTime = new Date().getTime();
        ZookeeperFactory zookeeperFactory = new DefaultZookeeperFactory();
        try {
            ZooKeeper zooKeeper = zookeeperFactory.newZooKeeper("127.0.0.1:2181", 5000, new Watcher() {
                public void process(WatchedEvent event) {
                    logger.info("{} enter process,state is {}", event.getType(), event.getState());
                    if (KeeperState.SyncConnected == event.getState()) {
                        //客户端处于连接状态
                        logger.info("state is syncConnected, do countDown");
                    }
                }
            }, false);
            logger.info("主线程进入等待状态...等待zookeeper 连接成功");
            logger.info("创建连接花费时间 {} 毫秒", new Date().getTime() - startTime);
            logger.info("连接状态 {}", zooKeeper.getState());//CONNECTING
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
    }
}
