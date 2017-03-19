package com.csb;

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

public class CSBConsumer implements AutoCloseable {
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

    //    TODO: fix the issue with creating a non existing topic
//    TODO: make a list of registered topics which don't exist yet (to subscribe later)
    public void register(String topic, MessageHandler messageHandler) {
        logger.info(String.format("Registering message handler of type %s for topic %s", messageHandler.getClass(), topic));

        synchronized (topicsSync) {
            if (isTopicExists(topic)) {
                logger.info(String.format("Registering to existing topic '%s'", topic));
            } else {
                CSBTopicCreator.createTopicSync(zkConnectionString, topic, 1, 1);
                logger.info(String.format("Creating topic '%s'", topic));
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
        if (!activated) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            start();
        }
    }

    public void start() {
        if (activated) {
            throw new IllegalAccessError("Start can be called only once");
        }
        if (consumer.subscription().isEmpty()) {
            return; // Can't actually start until subscribe to at least one topic
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

    @Override
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
