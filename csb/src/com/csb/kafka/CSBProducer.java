package com.csb.kafka;

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

    public void sendMsg(String topic, String msg) throws Exception {
        logger.info(String.format("Sending message '%s' to topic '%s'", msg, topic));
        try {
            Future<RecordMetadata> data = producer.send(new ProducerRecord<>(topic, msg));
            RecordMetadata metadata = data.get();
            logger.info("Sending message completed - " + metadata.toString());
        } catch (Exception e) {
            logger.error(String.format("Exception 'e' on sending message '%s' to topic '%s'", e.getMessage(), msg, topic));
            throw e;
        }
	}

    public void sendMsgNoWait(String topic, String msg) throws Exception {
        sendMsg(topic, msg);
        producer.flush();
    }

	public void close() {
        logger.info("Closing the consumer");
        producer.close();
        logger.info("Consumer closed");
    }
}
