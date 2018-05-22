package com.ycz.zk;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.AsyncCallback.DataCallback;
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

//异步版本
public class ClientApiGetDataAsyncTest {

    private static final Logger logger = LoggerFactory.getLogger(ClientApiGetDataAsyncTest.class);

    public static void main(String[] args) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ClientApiGetDataAsyncConnectedWatcher connectedWatcher = new ClientApiGetDataAsyncConnectedWatcher();
        connectedWatcher.setCountDownLatch(countDownLatch);
        ClientApiGetDataAsyncWatcher watcher = new ClientApiGetDataAsyncWatcher();
        ClientApiGetDataAsyncCallback callback = new ClientApiGetDataAsyncCallback();
        String address = "127.0.0.1:2181";
        Stat stat = new Stat();
        try {
            ZooKeeper zooKeeper = new ZooKeeper(address, 5000, connectedWatcher);
            countDownLatch.await();
            watcher.setZookeeper(zooKeeper);
            String path = "/test-getdata";
            zooKeeper.create(path, "data".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            zooKeeper.getData(path, watcher, callback, "Transmission content");
            zooKeeper.setData(path, "666".getBytes(), -1); // data 其实能保存一个序列化的对象,然后使用的时候反序列化回来
            Thread.sleep(10000);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}

class ClientApiGetDataAsyncWatcher implements Watcher {

    private static final Logger logger = LoggerFactory.getLogger(ClientApiGetDataAsyncWatcher.class);
    private ZooKeeper zookeeper;
    private Stat stat = new Stat();

    public void setZookeeper(ZooKeeper zookeeper) {
        this.zookeeper = zookeeper;
    }

    public void process(WatchedEvent event) {
        if (event.getState() == KeeperState.SyncConnected) {
            if (event.getType() == EventType.NodeDataChanged) {
                logger.info("数据发生变化 {}", event.getPath());
                try {
                    logger.info("{} 监听数据通知内容", new String(zookeeper.getData(event.getPath(), true, stat)));
                    logger.info("监听通知内容stat:czxid {}, mzxid {}, version {}", stat.getCzxid(), stat.getMzxid(), stat.getVersion());
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

class ClientApiGetDataAsyncConnectedWatcher implements Watcher {

    private static final Logger logger = LoggerFactory.getLogger(ClientApiGetDataAsyncConnectedWatcher.class);
    private CountDownLatch countDownLatch;

    public void setCountDownLatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    public void process(WatchedEvent event) {
        if (event.getState() == KeeperState.SyncConnected) {
            if (event.getType() == EventType.None && event.getPath() == null) {
                logger.info("创建连接成功");
                countDownLatch.countDown();
            }
        }
    }
}

class ClientApiGetDataAsyncCallback implements DataCallback {

    private static final Logger logger = LoggerFactory.getLogger(ClientApiGetDataAsyncCallback.class);

    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        logger.info("异步返回结果：rc=" + rc + ";path=" + path + ";data=" + new String(data));
        logger.info("异步读取Stat：czxid=" + stat.getCzxid()
            + ";mzxid=" + stat.getMzxid() + ";version=" + stat.getVersion());
    }
}