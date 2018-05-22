package com.ycz.zk;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
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

//同步版本
public class ClientApiGetDataTest {

    private static final Logger logger = LoggerFactory.getLogger(ClientApiGetDataTest.class);

    public static void main(String[] args) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ClientApiGetDataConnectedWatcher getDataConnectedWatcher = new ClientApiGetDataConnectedWatcher();
        getDataConnectedWatcher.setCountDownLatch(countDownLatch);
        ClientApiGetDataWatcher getDataWatcher = new ClientApiGetDataWatcher();
        try {
            ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2181", 5000, getDataConnectedWatcher);
            countDownLatch.await(); //等待连接创建成功
            getDataWatcher.setZooKeeper(zooKeeper);
            String path = "/api_getdata";
            zooKeeper.create(path, "123".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            Stat stat = new Stat();
            logger.info("获取到的data {}", zooKeeper.getData(path, getDataWatcher, stat));
            zooKeeper.setData(path, "321".getBytes(), -1);
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

class ClientApiGetDataConnectedWatcher implements Watcher {

    private static final Logger logger = LoggerFactory.getLogger(ClientApiGetDataConnectedWatcher.class);
    private CountDownLatch countDownLatch;

    public void setCountDownLatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    public void process(WatchedEvent event) {
        //状态连接上
        if (event.getState() == KeeperState.SyncConnected) {
            if (event.getType() == EventType.None && event.getPath() == null) {
                //创建连接成功了
                logger.info("创建连接成功");
                countDownLatch.countDown();
            }
        }
    }
}

class ClientApiGetDataWatcher implements Watcher {

    private static final Logger logger = LoggerFactory.getLogger(ClientApiGetDataWatcher.class);

    private ZooKeeper zooKeeper;
    private Stat stat = new Stat();

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    public void process(WatchedEvent event) {
        //状态连接上
        if (event.getState() == KeeperState.SyncConnected) {
            if (event.getType() == EventType.NodeDataChanged) {
                //节点的数据产生了变化
                try {
                    logger.info("{} 监听获取到的内容", zooKeeper.getData(event.getPath(), true, stat));
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