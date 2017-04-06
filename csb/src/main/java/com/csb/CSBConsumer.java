package com.csb;

import common.Environment;
import common.PropertiesLoader;
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

    private final int DEFAULT_SLEEP_MS = 100;
    private final HashMap<String, List<MessageHandler>> topicToHandlers = new HashMap<>();
    private final KafkaConsumer<String, String> consumer;
    private final CSBTopicCreator topicCreator;

    private boolean started;
    private boolean closed;

    public CSBConsumer(Environment environment, String groupId) {
        Properties prop = PropertiesLoader.loadProperties(PropertiesLoader.CONSUMER, environment);
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumer = new KafkaConsumer<>(prop);
        topicCreator = new CSBTopicCreator(Environment.PRODUCTION);
    }

    public void subscribe(String topic, MessageHandler messageHandler) {
        if (messageHandler == null) {
            throw new NullPointerException("Can't add a null message handler");
        }
        if (topic == null || topic.isEmpty()) {
            throw new NullPointerException("Topic can't be null or empty");
        }

        logger.info(String.format("Registering message handler of type %s for topic %s", messageHandler.getClass(), topic));

        synchronized (handlersSync) {
            List<MessageHandler> handlers = topicToHandlers.computeIfAbsent(topic, k -> new LinkedList<>());
            handlers.add(messageHandler);
        }

        synchronized (consumerSync) {
            createIfTopicMissing(topic);
            subscribeConsumerIfNeeded(topic);
        }
    }

    public void start() {
        validateCanBeCalled();
        started = true;

        new Thread(() -> {
            waitUntilRegistered();
            ConsumerRecords<String, String> records;
            while (!closed) {
                sleep(DEFAULT_SLEEP_MS);
                synchronized (consumerSync) {
                    if (closed) {
                        break;
                    }
                    records = consumer.poll(100);
                }
                for (ConsumerRecord<String, String> record : records) {
                    handleMessage(record.topic(), record.value());
                }
            }
        }).start();
    }

    private void validateCanBeCalled() {
        if (started) {
            throw new IllegalAccessError("Start can be called only once");
        }
        if (closed) {
            throw new IllegalAccessError("Can't call start after the consumer was closed");
        }
    }

    public Set<String> getAvailableTopics() {
        synchronized (consumerSync) {
            return consumer.listTopics().keySet();
        }
    }

    public Set<String> getSubscribedTopics() {
        synchronized (consumerSync) {
            return consumer.subscription();
        }
    }

    private void subscribeConsumerIfNeeded(String topic) {
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

    private void createIfTopicMissing(String topic) {
        if (isTopicExists(topic)) {
            logger.info(String.format("Registering to existing topic '%s'", topic));
        } else {
            logger.info(String.format("Trying to create topic '%s'", topic));
            if (!topicCreator.createTopicSync(topic)) {
                logger.error(String.format("Unable to create topic '%s'", topic));
            } else {
                logger.info(String.format("Topic '%s' was created successfully", topic));
            }
        }
    }

    private void handleMessage(String topic, String data) {
        List<MessageHandler> handlers = topicToHandlers.get(topic);
        if (handlers == null) {
            logger.error(String.format("Received record on topic '%s' with handlers empty", topic));
            return;
        }
        synchronized (handlersSync) {
            for (MessageHandler handler : handlers) {
                handler.handle(data);
            }
        }
    }

    private void sleep(int sleepMS) {
        try {
            Thread.sleep(sleepMS);
        } catch (InterruptedException e) {
            logger.error(String.format("Exception during sleep '%s'", e.getMessage()));
            e.printStackTrace();
        }
    }

    private void waitUntilRegistered() {
        boolean subscriptionsAreEmpty = true;
        while (subscriptionsAreEmpty) {
            sleep(DEFAULT_SLEEP_MS);
            synchronized (consumerSync) {
                subscriptionsAreEmpty = consumer.subscription().isEmpty();
            }
        }
    }

    private boolean isTopicExists(String topic) {
        Map<String, List<PartitionInfo>> serverTopics = consumer.listTopics();
        return serverTopics.containsKey(topic);
    }

    public boolean isStarted() {
        return started;
    }

    @Override
    public void close() {
        if (!started) {
            throw new IllegalAccessError("Can't close without calling start");
        }
        closed = true;

        synchronized (consumerSync) {
            consumer.close();
        }
    }
}
