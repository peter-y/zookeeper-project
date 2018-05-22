package com.ycz.zk;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientApiAllWatcherTest {

    private static final Logger logger = LoggerFactory.getLogger(ClientApiAllWatcherTest.class);

    public static void main(String[] args) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ClientApiAllWatcherConnectedW1 connectedW1 = new ClientApiAllWatcherConnectedW1();
        connectedW1.setCountDownLatch(countDownLatch);
        ClientApiAllWatcherW1 watcherW1 = new ClientApiAllWatcherW1();
        ClientApiAllWatcherW2 watcherW2 = new ClientApiAllWatcherW2();
        Stat stat = new Stat();
        try {
            ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2181", 5000, connectedW1);
            countDownLatch.await();
            String path = "/api_watcher";
            zooKeeper.create(path, "watcher".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL); //临时节点不能创建子节点
            logger.info("get data");
            zooKeeper.getData(path, watcherW1, stat); // 只会被触发一次 data的watcher 就是对内容变化的监听
            logger.info("set data no1");
            zooKeeper.setData(path, "www1".getBytes(), -1);
            logger.info("set data no2");
            zooKeeper.setData(path, "www2".getBytes(), -1);
            logger.info("set data no3");
            zooKeeper.setData(path, "www3".getBytes(), -1);
            String prepath = "/api_test";
            zooKeeper.create(prepath, "apitest".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); //存在会抛出异常
            zooKeeper.getChildren(prepath, watcherW1);//children 注册观察 就是对子节点的监听
            zooKeeper.setData(prepath, "abc".getBytes(), -1);
            logger.info("create children path /t1");
            zooKeeper.create(prepath + "/t1", "t1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            logger.info("create children path /t2");
            zooKeeper.create(prepath + "/t2", "t2".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            zooKeeper.exists(prepath, true);// 直接true 是使用client 注册的监听,这个操作注册的是 data变化的事件
            logger.info("create children path /t3");
            zooKeeper.create(prepath + "/t3", "t3".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            logger.info("set data");
            zooKeeper.setData(prepath, "ooo".getBytes(), -1);
            Thread.sleep(10000);
            zooKeeper.delete(prepath, -1); //不能级联删除
            logger.info("execute end");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}


class ClientApiAllWatcherConnectedW1 implements Watcher {

    private static final Logger logger = LoggerFactory.getLogger(ClientApiAllWatcherConnectedW1.class);
    private CountDownLatch countDownLatch;

    public void setCountDownLatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    public void process(WatchedEvent event) {
        logger.info("连接成功的事件被触发");
        if (event.getState() == KeeperState.SyncConnected) {
            logger.info("连接成功");
            countDownLatch.countDown();
        }
    }
}

class ClientApiAllWatcherW1 implements Watcher {

    private static final Logger logger = LoggerFactory.getLogger(ClientApiAllWatcherW1.class);

    public void process(WatchedEvent event) {
        logger.info("事件被触发1 {}", event.getPath());
    }
}

class ClientApiAllWatcherW2 implements Watcher {

    private static final Logger logger = LoggerFactory.getLogger(ClientApiAllWatcherW2.class);

    public void process(WatchedEvent event) {
        logger.info("事件被触发2");
    }
}
