package com.ycz.curator.leader;

import java.util.ArrayList;
import java.util.List;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderSelectorTest {

    private static final Logger logger = LoggerFactory.getLogger(LeaderSelectorTest.class);
    private static final String Path = "/demo/leader";
    private static final String address = "127.0.0.1:2181";

    /**
     * 最终效果，client会按照某种顺序去申请成为leader，结果上看，每个client都有可能成为一段时间的leader.
     * @param args
     */
    public static void main(String[] args) {
        List<LeaderSelector> selectors = new ArrayList<LeaderSelector>();
        List<CuratorFramework> clients = new ArrayList<CuratorFramework>();
        try {
            for (int i = 0; i < 10; i++) {
                CuratorFramework curatorFramework = getClient();
                clients.add(curatorFramework);
                final String clientname = "client#" + i;
                LeaderSelector leaderSelector = new LeaderSelector(curatorFramework, Path, new LeaderSelectorListener() {
                    public void takeLeadership(CuratorFramework client) throws Exception {
                        logger.info("{} I am leader;take leadership", clientname);
                        Thread.sleep(2000);
                    }

                    public void stateChanged(CuratorFramework client, ConnectionState newState) {
                        logger.info("{} {} state changed", clientname, newState.name());
                    }
                });
                leaderSelector.autoRequeue();//自动重新排队，理解是client会重新申请leader的权限
                leaderSelector.start();
                selectors.add(leaderSelector);
            }
            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            for (CuratorFramework client : clients) {
                CloseableUtils.closeQuietly(client);
            }
            for (LeaderSelector selector : selectors) {
                CloseableUtils.closeQuietly(selector);
            }
        }
    }

    private static CuratorFramework getClient() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder()
            .namespace("demo")
            .retryPolicy(retryPolicy)
            .connectionTimeoutMs(3000)
            .connectString(address)
            .sessionTimeoutMs(5000)
            .build();
        client.start();
        return client;
    }
}
