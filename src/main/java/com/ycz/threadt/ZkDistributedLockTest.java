package com.ycz.threadt;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkDistributedLockTest {

    private static final Logger logger = LoggerFactory.getLogger(ZkDistributedLockTest.class);

    //静态变量模拟公共资源
    private static int counter = 0;

    //操作计数
    public static void plus() {
        //计数器+1 counter原本是线程不安全的
        counter++;
        logger.info("counter {}", counter);
        try {
            Thread.sleep(2000);//当有处理时长的时候 并发问题出现
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static class CountPlus extends Thread {

        private CountDownLatch countDownLatch;
        private static final String LOCK_ROOT_PATH = "/Locks";
        private static final String LOCK_ROOT_NAME = "Lock_";

        ZooKeeper zooKeeper;

        @Override
        public void run() {
            for (int i = 0; i < 20; i++) {
                String lockPath = getLock();
                plus();
                releaseLock(lockPath);

            }
            closeZkClient();
            countDownLatch.countDown();
            logger.info("{} 线程执行完毕 {}", Thread.currentThread().getName(), counter);
        }

        public CountPlus(String name, CountDownLatch countDownLatch) {
            super(name);
            this.countDownLatch = countDownLatch;
        }

        private String getLock() {
            try {
                String lockPath = zooKeeper
                    .create(LOCK_ROOT_PATH + "/" + LOCK_ROOT_NAME, Thread.currentThread().getName().getBytes(), Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL_SEQUENTIAL);
                logger.info("{} create path {}", Thread.currentThread().getName(), lockPath);
                tryLock(lockPath);
                return lockPath;
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        }


        /**
         * 每个线程创建 有序node，最早创建的线程就能拿到列首的节点信息，在这个节点信息一直存在的情况下，其他线程进入都会进入等待状态<br>
         * 并且拿到上一个node的watcher,当上一个node执行完成被释放删除之后，观察出发通知所有进入wait的线程
         */
        private boolean tryLock(String lockPath) throws KeeperException, InterruptedException {
            List<String> children = zooKeeper.getChildren(LOCK_ROOT_PATH, false);
            Collections.sort(children);
            int index = children.indexOf(lockPath.substring(LOCK_ROOT_PATH.length() + 1));
            if (index == 0) {
                logger.info("{} get lock,lockPath {}", Thread.currentThread().getName(), lockPath);
                return true;
            } else {
                Watcher watcher = new Watcher() {
                    public void process(WatchedEvent event) {
                        logger.info("{} 删除持有的 {}", Thread.currentThread().getName(), event.getPath());
                        synchronized (this) {
                            notifyAll();
                        }
                    }
                };
                String preLockPath = children.get(index - 1);
                Stat stat = zooKeeper.exists(LOCK_ROOT_PATH + "/" + preLockPath, watcher);
                logger.info("{} 观察 {}", Thread.currentThread().getName(), LOCK_ROOT_PATH + "/" + preLockPath);
                if (stat == null) {
                    logger.info("{} 观察 {} stat is null", Thread.currentThread().getName(), LOCK_ROOT_PATH + "/" + preLockPath);
                    return tryLock(lockPath);
                } else {
                    logger.info("{} wait for {}", Thread.currentThread().getName(), preLockPath);
                    synchronized (watcher) {
                        watcher.wait();
                    }
                    return tryLock(lockPath);
                }
            }
        }

        private void releaseLock(String lockPath) {
            try {
                //-1 忽略版本号 path stat 中 拥有相应的版本号，类似乐观锁
                zooKeeper.delete(lockPath, -1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            }
        }

        public void setZooKeeper(ZooKeeper zooKeeper) {
            this.zooKeeper = zooKeeper;
        }

        public void closeZkClient() {
            try {
                zooKeeper.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        Watcher clientwatcher = new Watcher() {
            public void process(WatchedEvent event) {
                logger.info("main client watcher {}", event.getPath());
            }
        };
        ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2181", 3000, clientwatcher);
        Watcher watcher = new Watcher() {
            public void process(WatchedEvent event) {
                logger.info("exists root watcher {}", event.getPath());
            }
        };
        try {
            Stat stat = zooKeeper.exists("/Locks", watcher);
            if (stat == null) {
                zooKeeper.create("/Locks", "/Locks".getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
                logger.info("没有锁的根路径，创建");
            } else {
                logger.info("锁的根路径已经存在");
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        CountDownLatch countDownLatch = new CountDownLatch(2);
        CountPlus t1 = new CountPlus("线程1", countDownLatch);
        setZkClient(t1, "127.0.0.1:2181");
        t1.start();
        CountPlus t2 = new CountPlus("线程2", countDownLatch);
        setZkClient(t2, "127.0.0.1:2181");
        t2.start();
        /*CountPlus t3 = new CountPlus("线程3", countDownLatch);
        setZkClient(t3, "127.0.0.1:2182");
        t3.start();
        CountPlus t4 = new CountPlus("线程4", countDownLatch);
        setZkClient(t4, "127.0.0.1:2182");
        t4.start();
        CountPlus t5 = new CountPlus("线程5", countDownLatch);
        setZkClient(t5, "127.0.0.1:2183");
        t5.start();
        CountPlus t6 = new CountPlus("线程6", countDownLatch);
        setZkClient(t6, "127.0.0.1:2183");
        t6.start();*/
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("最终计数值为 {}", counter);
    }

    public static void setZkClient(CountPlus client, String zkIp) throws IOException {
        Watcher watcher = new Watcher() {
            public void process(WatchedEvent event) {
                logger.info("zkClient create watcher");
            }
        };
        ZooKeeper zooKeeper = new ZooKeeper(zkIp, 3000, watcher);
        client.setZooKeeper(zooKeeper);
    }

}
