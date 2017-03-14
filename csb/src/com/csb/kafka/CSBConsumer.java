package com.csb.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class CSBConsumer {
    final static Logger logger = Logger.getLogger(CSBConsumer.class);
    final private Object consumeSync = new Object();

    final private HashMap<String, List<MessageHandler>> topicToHandlers = new HashMap<>();
    final private KafkaConsumer<String, String> consumer;
    private boolean activated;
    private boolean closed;

    public CSBConsumer() {
        Properties prop = PropertiesLoader.loadProperties(PropertiesLoader.CONSUMER_DEV);
        consumer = new KafkaConsumer<>(prop);
    }

    public CSBConsumer(String groupId) {
        Properties prop = PropertiesLoader.loadProperties(PropertiesLoader.CONSUMER_DEV);
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumer = new KafkaConsumer<>(prop);
    }

    public void register(String topic, MessageHandler messageHandler) {
        logger.info(String.format("Registering message handler of type %s for topic %s", messageHandler.getClass(), topic));

        synchronized (CSBConsumer.class) {
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
                synchronized (consumeSync) {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        String topic = record.topic();
                        List<MessageHandler> handlers = topicToHandlers.get(topic);
                        if (handlers == null) {
                            logger.error(String.format("Received record on topic '%s' with handlers empty", topic));
                            continue;
                        }
                        synchronized (CSBConsumer.class) {
                            for (MessageHandler handler : handlers) {
                                handler.handle(record.value());
                            }
                        }
                    }
                }
            }
        }).start();
    }

    public void close() {
        if (!activated) {
            throw new IllegalAccessError("Can't close without calling start");
        }
        closed = true;

        synchronized (consumeSync) {
            consumer.close();
        }
    }
}
