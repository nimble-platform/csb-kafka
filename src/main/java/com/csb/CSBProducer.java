package com.csb;

import common.Environment;
import common.PropertiesLoader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.Future;

public class CSBProducer implements AutoCloseable {
    final static Logger logger = Logger.getLogger(CSBProducer.class);

    private Producer<String, String> producer = null;

    public CSBProducer(Environment environment) {
        Properties props = PropertiesLoader.loadProperties(PropertiesLoader.PRODUCER, environment);
        producer = new KafkaProducer<>(props);
    }

    private boolean sendMsg(String topic, String msg, boolean immediate) {
        logger.info(String.format("Trying to send message '%s' to topic '%s'", msg, topic));
        try {
            Future<RecordMetadata> data = producer.send(new ProducerRecord<>(topic, msg));
            if (!immediate) {
                logger.info("Putting message in async send - completed");
            } else {
                RecordMetadata metadata = data.get();
                logger.info("Message was sent on - " + String.valueOf(metadata.checksum()));
            }
            return true;
        } catch (Exception e) {
            logger.error(String.format("Exception '%s' on sending message '%s' to topic '%s'", e.getMessage(), msg, topic));
            return false;
        }
    }

    public boolean sendMsgInBulk(String topic, String msg) {
        return sendMsg(topic, msg, false);
    }

    public boolean sendMsgNoWait(String topic, String msg) {
        return sendMsg(topic, msg, true);
    }

    @Override
    public void close() {
        logger.info("Closing the consumer");
        producer.close();
        logger.info("Consumer closed");
    }
}
