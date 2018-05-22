package com.ycz.zk;

import java.io.IOException;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkClientTest {

    private static final Logger logger = LoggerFactory.getLogger(ZkClientTest.class);

    private ZooKeeper zooKeeper;
    //ephemeral_sequential
    public void createPath(String path) {
        try {
            //PERSISTENT_SEQUENTIAL 会在后面加0000000001 这种编号
            //PERSISTENT NODE 存在的话 创建报错
            //不支持一次创建多级
            zooKeeper.create(path, path.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void deletePath(String path) {
        try {
            zooKeeper.delete(path, 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public void getPath(String path) {
        try {
            List<String> list = zooKeeper.getChildren(path, new Watcher() {
                public void process(WatchedEvent event) {
                    logger.info("zookeeper getChildren watched", event.toString());
                }
            });
            for (String p : list) {
                logger.info("path is {}", p);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void existsPath(String path) {
        Watcher watcher = new Watcher() {
            public void process(WatchedEvent event) {
                logger.info("exists watcher path {}", event.getPath());
            }
        };
        try {
            Stat stat = zooKeeper.exists(path, watcher);
            if (stat == null) {
                logger.info("stat is null");
            } else {
                logger.info("stat is {}", stat.getMzxid());
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    public void close() {
        try {
            zooKeeper.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        ZkClientTest zkClientTest = new ZkClientTest();
        ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2181", 3000, new Watcher() {
            public void process(WatchedEvent event) {
                logger.info("zookeeper client watched", event.toString());
            }
        });
        zkClientTest.setZooKeeper(zooKeeper);
        zkClientTest.existsPath("/test1");
        zkClientTest.existsPath("/test");
        zkClientTest.close();
    }
}
