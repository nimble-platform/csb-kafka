package com.csb.topics;

import common.Environment;
import common.PropertiesLoader;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.log4j.Logger;

import java.util.Properties;

//TODO: refactor to use rest admin
//TODO: load the list of topics on start
public class CSBZookeeperTopicsHandler {
    private final static Logger logger = Logger.getLogger(CSBZookeeperTopicsHandler.class);

    private final static Object completeSync = new Object();
    final private int sessionTimeoutMs;
    final private int connectionTimeout;
    final private int partitions;
    final private int replications;

    final private String connectionString;

    public CSBZookeeperTopicsHandler(Environment environment) {
        Properties prop = PropertiesLoader.loadProperties(PropertiesLoader.TOPIC_CREATOR, environment);

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
                logger.info(String.format("Topic by the name '%s' already exists - will not create a new", topicName));
                return false;
            }

            SyncTopicCreator creator = new SyncTopicCreator(zkUtils, topicName, partitions, replications);
            new Thread(creator).start();
            synchronized (completeSync) {
                completeSync.wait();
            }
            logger.info("Topic created");
            return true;
        } catch (Exception e) {
            logger.error(String.format("Exception on creating topic '%s' ", topicName), e);
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
