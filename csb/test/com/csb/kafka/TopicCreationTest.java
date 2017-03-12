package com.csb.kafka;

public class TopicCreationTest {

  public static void main(String[] args) {
	  /*
    String zookeeperConnect = "localhost:2181";
    int sessionTimeoutMs = 10 * 1000;
    int connectionTimeoutMs = 8 * 1000;
    // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
    // createTopic() will only seem to work (it will return without error).  The topic will exist in
    // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
    // topic.
    ZkClient zkClient = new ZkClient(
        zookeeperConnect,
        sessionTimeoutMs,
        connectionTimeoutMs,
        ZKStringSerializer$.MODULE$);

    // Security for Kafka was added in Kafka 0.9.0.0
    boolean isSecureKafkaCluster = false;
    ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);

    String topic = "my-topic2";
    int partitions = 1;
    int replication = 1;
    Properties topicConfig = new Properties(); // add per-topic configurations settings here
    AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig, null);
    zkClient.close();
    */
    CSBTopicCreator csbTopicCreator = new CSBTopicCreator();
	String topic = "my-topic5";
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
