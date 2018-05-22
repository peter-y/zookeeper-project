package com.ycz.zk;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.curator.utils.DefaultZookeeperFactory;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientApiCreateTest {

    private static final Logger logger = LoggerFactory.getLogger(ClientApiCreateTest.class);
    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    public static void main(String[] args) {
        try {
            ApiTestWatcher watcher = new ApiTestWatcher();
            watcher.setCountDownLatch(countDownLatch);
            ZooKeeper zooKeeper =
                new DefaultZookeeperFactory().newZooKeeper("127.0.0.1:2181", 5000, watcher, false);
            countDownLatch.await();
            //创建临时节点
            String path = zooKeeper.create("/api_test_create", "data".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            logger.info("创建路径成功 {}", path);
            path = zooKeeper.create("/api_test_create", "data".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            logger.info("创建路径成功 {}", path);
            //异步创建节点
            zooKeeper
                .create("/api_test_create_asynchronous", "data".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                    new ApiTestStringCallBack(),
                    "what");

            zooKeeper
                .create("/api_test_create_asynchronous", "data".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL,
                    new ApiTestStringCallBack(),
                    "what");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

//观察者
class ApiTestWatcher implements Watcher {

    private static final Logger logger = LoggerFactory.getLogger(ApiTestWatcher.class);
    private CountDownLatch countDownLatch;

    public void setCountDownLatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    public void process(WatchedEvent event) {
        logger.info("{} enter process,state is {}", event.getType(), event.getState());
        if (KeeperState.SyncConnected == event.getState()) {
            //客户端处于连接状态
            logger.info("state is syncConnected, do countDown");
            countDownLatch.countDown();
        }
    }
}

//异步回调
class ApiTestStringCallBack implements StringCallback {

    private static final Logger logger = LoggerFactory.getLogger(ApiTestStringCallBack.class);

    public void processResult(int rc, String path, Object ctx, String name) {
        logger.info("异步创建回调结果: 状态 {} , 创建路径 {} , 传递信息 {} , 实际节点名称 {}",
            rc, path, ctx, name);
    }
}
