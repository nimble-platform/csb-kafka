package com.csb.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.util.*;

public class CSBConsumer {
    final static Logger logger = Logger.getLogger(CSBConsumer.class);

    final private HashMap<String, List<MessageHandler>> topicToHandlers = new HashMap<>();
    final private KafkaConsumer<String, String> consumer;
    private boolean started;

    public CSBConsumer() {
        Properties prop = PropertiesLoader.loadProperties(PropertiesLoader.CONSUMER_DEV);
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
        if (started) {
            throw new IllegalAccessError("Start can be called only once");
        }
        started = true;

        new Thread(() -> {
            while (true) {
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
        }).start();
    }

    private class Consumer implements Runnable {
        @Override
        public void run() {

        }
    }

    public static void main(String[] args) {
        BasicConfigurator.configure();
//		Properties props = new Properties();
//		props.put("bootstrap.servers", "localhost:9092");
//		props.put("group.id", "group-1");
//		props.put("enable.auto.commit", "true");
//		props.put("auto.commit.interval.ms", "1000");
//		props.put("auto.offset.reset", "earliest");
//		props.put("session.timeout.ms", "30000");
//		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

//        Properties prop = PropertiesLoader.loadProperties(PropertiesLoader.CONSUMER_DEV);
//        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(prop);
//
//        //CSBConsumer csbConsumer = new CSBConsumer();
//        kafkaConsumer.subscribe(Arrays.asList("test-topic1"));
//        kafkaConsumer.subscribe(Arrays.asList("my-topic2"));

    }
}
