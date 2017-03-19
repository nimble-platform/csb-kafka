package com.csb;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.Properties;

public class CSBTopicCreator {
    final private int sessionTimeoutMs = 10 * 1000;
    final private int connectionTimeout = 5 * 1000;
    final private String connectionString;

    public CSBTopicCreator(String connectionString) {
        this.connectionString = connectionString;
    }

    //    TODO: update the isSecure field
    public void createTopic(String topicName, int partitions, int replications) {
        ZkClient zkClient = new ZkClient(connectionString, sessionTimeoutMs, connectionTimeout, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(connectionString), false);

        try {
            Properties topicConfig = new Properties(); // add per-topic

            if (!AdminUtils.topicExists(zkUtils, topicName)) {
                try {
                    AdminUtils.createTopic(zkUtils, topicName, partitions, replications, topicConfig, null);
                    System.out.println("Topic created. name: {" + topicName + "}, partitions: {" + partitions
                            + "}, replication: {" + replications + "}");
                } catch (TopicExistsException ignore) {
                    System.out.println("Topic exists. name: {" + topicName + "}");
                }
            } else {
                System.out.println("Topic exists. name: {" + topicName + "}");
            }
        } finally {
            zkUtils.close();
            zkClient.close();
        }
    }
}
