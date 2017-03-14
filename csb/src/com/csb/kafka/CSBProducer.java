package com.csb.kafka;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.Future;

public class CSBProducer {
    final static Logger logger = Logger.getLogger(CSBProducer.class);

    private Producer<String, String> producer = null;

    public CSBProducer() {
        Properties props = PropertiesLoader.loadProperties(PropertiesLoader.PRODUCER_DEV);
        producer = new KafkaProducer<>(props);
    }

    public void sendMsg(String topic, String msg, boolean immediate) throws Exception {
        logger.info(String.format("Sending message '%s' to topic '%s'", msg, topic));
        try {
            Future<RecordMetadata> data = producer.send(new ProducerRecord<>(topic, msg));
            if (!immediate) {
                logger.info("Sending message completed");
            } else {
                RecordMetadata metadata = data.get();
                logger.info("Sending message completed - " + metadata.toString());
            }
        } catch (Exception e) {
            logger.error(String.format("Exception '%s' on sending message '%s' to topic '%s'", e.getMessage(), msg, topic));
            throw e;
        }
    }

    public void sendMsgNoWait(String topic, String msg) throws Exception {
        sendMsg(topic, msg, true);
        producer.flush();
    }

    private void createTopic(String topicName) {
        ZkClient zkClient = null;
        ZkUtils zkUtils = null;
        try {
            String zookeeperHosts = "192.168.56.101:2181"; // If multiple zookeeper then -> String zookeeperHosts = "192.168.20.1:2181,192.168.20.2:2181";
            int sessionTimeOutInMs = 15 * 1000; // 15 secs
            int connectionTimeOutInMs = 10 * 1000; // 10 secs

            zkClient = new ZkClient(zookeeperHosts, sessionTimeOutInMs, connectionTimeOutInMs, ZKStringSerializer$.MODULE$);
            zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHosts), false);

            int noOfPartitions = 2;
            int noOfReplication = 3;
            Properties topicConfiguration = new Properties();

            AdminUtils.createTopic(zkUtils, topicName, noOfPartitions, noOfReplication, topicConfiguration, null);

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }

    public void close() {
        logger.info("Closing the consumer");
        producer.close();
        logger.info("Consumer closed");
    }
}
