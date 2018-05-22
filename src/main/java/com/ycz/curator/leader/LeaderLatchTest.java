package com.ycz.curator.leader;

import java.util.ArrayList;
import java.util.List;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderLatchTest {

    private static final Logger logger = LoggerFactory.getLogger(LeaderLatchTest.class);

    private static final String PATH = "/demo/leader";

    /**
     * 某个client 申请成为leader，当出现问题时？才会释放leader的权利.
     * @param args
     */
    public static void main(String[] args) {
        logger.info("print leaderLatch main");
        List<LeaderLatch> leaderLatches = new ArrayList<LeaderLatch>();
        List<CuratorFramework> clients = new ArrayList<CuratorFramework>();
        try {
            for (int i = 0; i < 10; i++) {
                logger.info("for in {}", i);
                CuratorFramework client = getClient();
                clients.add(client);
                final LeaderLatch leaderLatch = new LeaderLatch(client, PATH, "client#" + i);
                leaderLatch.addListener(new LeaderLatchListener() {
                    public void isLeader() {
                        logger.info("{}:I am leader.I am doing job!", leaderLatch.getId());
                    }

                    public void notLeader() {
                        logger.info("{}:I am not leader.I will do nothing", leaderLatch.getId());
                    }
                });
                leaderLatches.add(leaderLatch);
                leaderLatch.start();
            }
            Thread.sleep(Integer.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            for (CuratorFramework client : clients) {
                CloseableUtils.closeQuietly(client);
            }
            for (LeaderLatch leaderLatch : leaderLatches) {
                CloseableUtils.closeQuietly(leaderLatch);
            }
        }

    }

    private static CuratorFramework getClient() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString("127.0.0.1:2181")
            .retryPolicy(retryPolicy)
            .sessionTimeoutMs(6000)
            .connectionTimeoutMs(3000)
            .namespace("demo")
            .build();
        client.start();
        return client;
    }
}
