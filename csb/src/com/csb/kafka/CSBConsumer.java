package com.csb.kafka;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class CSBConsumer {
    private final static Logger logger = Logger.getLogger(CSBConsumer.class);
    private final Object consumerSync = new Object();
    private final Object handlersSync = new Object();
    private final static Object topicsSync = new Object();

    private final HashMap<String, List<MessageHandler>> topicToHandlers = new HashMap<>();
    private final KafkaConsumer<String, String> consumer;
    private final String zkConnectionString;

    private boolean activated;
    private boolean closed;

    public CSBConsumer(String groupId) {
        Properties prop = PropertiesLoader.loadProperties(PropertiesLoader.CONSUMER_DEV);
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumer = new KafkaConsumer<>(prop);

        zkConnectionString = prop.getProperty("zookeeper.connection.string");
    }

    public void register(String topic, MessageHandler messageHandler) {
        logger.info(String.format("Registering message handler of type %s for topic %s", messageHandler.getClass(), topic));

        synchronized (topicsSync) {
            if (!isTopicExists(topic)) {
                throw new UnsupportedOperationException("Unable to register to non existing topic for now");
//                new Thread(() -> createTopic(topic)).start();
            }
        }

        synchronized (handlersSync) {
            List<MessageHandler> handlers = topicToHandlers.computeIfAbsent(topic, k -> new LinkedList<>());
            handlers.add(messageHandler);
        }

        Set<String> topics = consumer.subscription();
        if (topics.contains(topic)) {
            logger.info(String.format("The consumer is already subscribed to topic '%s'", topic));
        } else {
            Set<String> newTopics = new HashSet<>(topics);
            newTopics.add(topic);
            logger.info(String.format("Adding new topic '%s' to the consumer", topic));
            consumer.subscribe(newTopics);
        }
    }

    public void start() {
        if (topicToHandlers.size() == 0) {
            throw new IllegalAccessError("Must first subscribe to at least one topic");
        }
        if (activated) {
            throw new IllegalAccessError("Start can be called only once");
        }
        activated = true;

        new Thread(() -> {
            while (!closed) {
                synchronized (consumerSync) {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        String topic = record.topic();
                        List<MessageHandler> handlers = topicToHandlers.get(topic);
                        if (handlers == null) {
                            logger.error(String.format("Received record on topic '%s' with handlers empty", topic));
                            continue;
                        }
                        synchronized (handlersSync) {
                            for (MessageHandler handler : handlers) {
                                handler.handle(record.value());
                            }
                        }
                    }
                }
            }
        }).start();
    }

    private boolean isTopicExists(String topic) {
        Map<String, List<PartitionInfo>> serverTopics = consumer.listTopics();
        return serverTopics.containsKey(topic);
    }

    private void createTopic(String topic) {
        logger.info(String.format("Creating topic named '%s'", topic));
        ZkClient zkClient = null;
        try {
            zkClient = new ZkClient(zkConnectionString, 10000, 5000, ZKStringSerializer$.MODULE$);
            ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnectionString), false);
            int noOfPartitions = 1;
            int noOfReplication = 1;
            Properties topicConfiguration = new Properties();
            AdminUtils.createTopic(zkUtils, topic, noOfPartitions, noOfReplication, topicConfiguration, AdminUtils.createTopic$default$6());
        } catch (Exception ex) {
            logger.error(String.format("Exception '%s' on creating topic '%s'", ex.getMessage(), topic));
            ex.printStackTrace();
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }

    public boolean isActivated() {
        return activated;
    }

    public void close() {
        if (!activated) {
            throw new IllegalAccessError("Can't close without calling start");
        }
        closed = true;

        synchronized (consumerSync) {
            consumer.close();
        }
    }
}
