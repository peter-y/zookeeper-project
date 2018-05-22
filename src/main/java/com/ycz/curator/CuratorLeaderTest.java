package com.ycz.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 未完成的示例.
 */
public class CuratorLeaderTest {

    private static final Logger logger = LoggerFactory.getLogger(CuratorLeaderTest.class);
    private static final String zookeeper_address = "127.0.0.1:2181";
    private static final String zookeeper_curator_path = "/curator_test";

    public static void main(String[] args) {
        LeaderSelectorListener listener = new LeaderSelectorListener() {
            public void takeLeadership(CuratorFramework client) throws Exception {
                logger.info("{} take leadership", Thread.currentThread().getName());
                Thread.sleep(5000);
                logger.info("{} relinquish leadership", Thread.currentThread().getName());
            }

            public void stateChanged(CuratorFramework client, ConnectionState newState) {

            }
        };
    }

    private static void registerListener(LeaderSelectorListener listener) {
        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeper_address, new RetryNTimes(10, 5000));
        client.start();
        try {
            client.create().creatingParentContainersIfNeeded().forPath(zookeeper_curator_path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
