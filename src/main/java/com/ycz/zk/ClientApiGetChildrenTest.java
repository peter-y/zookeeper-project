package com.ycz.zk;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientApiGetChildrenTest {

    private static final Logger logger = LoggerFactory.getLogger(ClientApiGetChildrenTest.class);
    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    public static void main(String[] args) {
        try {
            ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2181", 5000, new Watcher() {
                public void process(WatchedEvent event) {
                    logger.info("zookeeper client watcher");
                }
            });

            zooKeeper.create("/api_test_create1", "test".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zooKeeper.create("/api_test_create1/c1", "test_c1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            List<String> childs = zooKeeper.getChildren("/api_test_create", new Watcher() {
                public void process(WatchedEvent event) {
                    //这段代码并不会被执行
                    logger.info("{} 同步 getChildren watcher", event.getPath());
                }
            });
            ApiGetChildrenWatcher watcher = new ApiGetChildrenWatcher();
            watcher.setCountDownLatch(countDownLatch);
            watcher.setZooKeeper(zooKeeper);
            zooKeeper.getChildren("/api_test_create1", watcher, new ApiGetChildren2Callback(), "发送的消息");
            zooKeeper.create("/api_test_create1/c2", "test_c1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            Thread.sleep(10000);
            logger.info("create end");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
        }
    }
}

class ApiGetChildrenWatcher implements Watcher {

    private static final Logger logger = LoggerFactory.getLogger(ApiGetChildrenWatcher.class);
    private CountDownLatch countDownLatch;
    private ZooKeeper zooKeeper;

    public void setCountDownLatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    public void process(WatchedEvent event) {
        if (KeeperState.SyncConnected == event.getState()) {
            if (EventType.None == event.getType() && null == event.getPath()) {
                countDownLatch.countDown();
            } else if (event.getType() == EventType.NodeChildrenChanged) {
                try {
                    logger.info("重新获取children ,注册监听 {}", zooKeeper.getChildren(event.getPath(), true));
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

class ApiGetChildren2Callback implements Children2Callback {

    private static final Logger logger = LoggerFactory.getLogger(ApiGetChildren2Callback.class);

    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
        logger.info("异步获得getChild结果 ,rc = {} , path = {} , ctx = {} , children = {} , stat = {}",
            rc, path, ctx, children, stat);
    }
}
