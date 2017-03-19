package com.csb;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.Properties;

public class CSBTopicCreator {
    private final static Object completeSync = new Object();
    final private static int sessionTimeoutMs = 10 * 1000;
    final private static int connectionTimeout = 5 * 1000;

    //    TODO: update the isSecure field
    public static void createTopicSync(String connectionString, String topicName, int partitions, int replications) {
        ZkClient zkClient = new ZkClient(connectionString, sessionTimeoutMs, connectionTimeout, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(connectionString), false);

        try {
            if (AdminUtils.topicExists(zkUtils, topicName)) {
                System.out.println("Topic exists. name: {" + topicName + "}");
            } else {
                SyncTopicCreator creator = new SyncTopicCreator(zkUtils, topicName, partitions, replications);
                new Thread(creator).start();

                synchronized (completeSync) {
                    completeSync.wait();
                }
                System.out.println("Topic created");
            }
        } catch (Exception e) {
            System.out.println(String.format("Exception on creating topic '%s' '%s'", topicName, e.getMessage()));
            e.printStackTrace();
        } finally {
            zkUtils.close();
            zkClient.close();
        }
    }

    private static class SyncTopicCreator implements Runnable {
        private final ZkUtils zkUtils;
        private final String topicName;
        private final int partitions;
        private final int replications;

        public SyncTopicCreator(ZkUtils zkUtils, String topicName, int partitions, int replications) {
            this.zkUtils = zkUtils;
            this.topicName = topicName;
            this.partitions = partitions;
            this.replications = replications;
        }

        @Override
        public void run() {
            synchronized (completeSync) {
                AdminUtils.createTopic(zkUtils, topicName, partitions, replications, new Properties(), null);
                completeSync.notify();
            }
        }
    }
}
