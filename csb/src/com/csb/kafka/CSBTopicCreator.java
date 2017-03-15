package com.csb.kafka;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.Properties;

public class CSBTopicCreator {

	ZkClient zkClient;
	ZkUtils zkUtils;

	public CSBTopicCreator() {
		String zookeeperConnect = "localhost:2181";
		int sessionTimeoutMs = 10 * 1000;
		int connectionTimeoutMs = 8 * 1000;
		// Note: You must initialize the ZkClient with ZKStringSerializer. If
		// you don't, then
		// createTopic() will only seem to work (it will return without error).
		// The topic will exist in
		// only ZooKeeper and will be returned when listing topics, but Kafka
		// itself does not create the
		// topic.
		zkClient = new ZkClient(zookeeperConnect, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);

		// Security for Kafka was added in Kafka 0.9.0.0
		boolean isSecureKafkaCluster = false;
		zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);

	}

	public void createTopic(String topicName, int numPartitions) {
		int replication = 1;
		Properties topicConfig = new Properties(); // add per-topic
													// configurations settings
													// here
		System.out.println("Creating topic: " + topicName);
		
		if (!AdminUtils.topicExists(zkUtils, topicName)) {
			try {
				AdminUtils.createTopic(zkUtils, topicName, numPartitions, replication, topicConfig, null);
				System.out.println("Topic created. name: {" + topicName + "}, partitions: {" + numPartitions
						+ "}, replication: {" + replication + "}");
			} catch (TopicExistsException ignore) {
				System.out.println("Topic exists. name: {" + topicName + "}");
			}
		} else {
			System.out.println("Topic exists. name: {" + topicName + "}");
		}
	}

	public void close() {
		zkClient.close();
		System.out.println("topic creator closed");
	}

	public static void main(String[] args) {
		/*
		 * String zookeeperConnect = "localhost:2181"; int sessionTimeoutMs = 10
		 * * 1000; int connectionTimeoutMs = 8 * 1000; // Note: You must
		 * initialize the ZkClient with ZKStringSerializer. If you don't, then
		 * // createTopic() will only seem to work (it will return without
		 * error). The topic will exist in // only ZooKeeper and will be
		 * returned when listing topics, but Kafka itself does not create the //
		 * topic. ZkClient zkClient = new ZkClient( zookeeperConnect,
		 * sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
		 * 
		 * // Security for Kafka was added in Kafka 0.9.0.0 boolean
		 * isSecureKafkaCluster = false; ZkUtils zkUtils = new ZkUtils(zkClient,
		 * new ZkConnection(zookeeperConnect), isSecureKafkaCluster);
		 */
		CSBTopicCreator csbTopicCreator = new CSBTopicCreator();
        String topic = "my_test_topic";
        /*
		 * int partitions = 1; int replication = 1; Properties topicConfig = new
		 * Properties(); // add per-topic // configurations settings // here
		 * AdminUtils.createTopic(zkUtils, topic, partitions, replication,
		 * topicConfig, null);
		 */
		// zkClient.close();
		csbTopicCreator.createTopic(topic, 1);
		csbTopicCreator.close();
	}

}
