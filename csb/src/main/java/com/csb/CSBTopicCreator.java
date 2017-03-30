package com.csb;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.Properties;

public class CSBTopicCreator {
    private final static Object completeSync = new Object();
    final private int sessionTimeoutMs;
    final private int connectionTimeout;
    final private int partitions;
    final private int replications;

    final private String connectionString;

    public CSBTopicCreator() {
        Properties prop = PropertiesLoader.loadProperties(PropertiesLoader.TOPIC_CREATOR_DEV);

        sessionTimeoutMs = Integer.parseInt(prop.getProperty("session.timeout.ms"));
        connectionTimeout = Integer.parseInt(prop.getProperty("connection.timeout.ms"));
        partitions = Integer.parseInt(prop.getProperty("kafka.partitions"));
        replications = Integer.parseInt(prop.getProperty("kafka.replications"));
        connectionString = prop.getProperty("zookeeper.connection.string");
    }

    //    TODO: update the isSecure field
    public boolean createTopicSync(String topicName) {
        ZkClient zkClient = new ZkClient(connectionString, sessionTimeoutMs, connectionTimeout, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(connectionString), false);

        try {
            if (AdminUtils.topicExists(zkUtils, topicName)) {
                System.out.println(String.format("Topic by the name '%s' already exists - will not create a new", topicName));
                return false;
            }

            SyncTopicCreator creator = new SyncTopicCreator(zkUtils, topicName, partitions, replications);
            new Thread(creator).start();
            synchronized (completeSync) {
                completeSync.wait();
            }
            System.out.println("Topic created");
            return true;
        } catch (Exception e) {
            System.out.println(String.format("Exception on creating topic '%s' '%s'", topicName, e.getMessage()));
            e.printStackTrace();
            return false;
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

        SyncTopicCreator(ZkUtils zkUtils, String topicName, int partitions, int replications) {
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
