package com.csb.kafka;

import org.junit.Test;

/**
 * Created by evgeniyh on 12/03/17.
 */
public class CSBTest {
    private final String TEST_TOPIC = "test_topic_c8f45cb1-94e9-4138-8eea-874aa4ec7b02";

    @Test
    public void testReceiveOneMessage() throws Exception {
        CSBConsumer consumer = new CSBConsumer();
        CSBProducer producer = new CSBProducer();

        final boolean[] message_received = {false};
        consumer.register(TEST_TOPIC, message -> message_received[0] = true);

        producer.sendMsg(TEST_TOPIC, "test_message");
        producer.close();
        consumer.start();

        while (true) {
            if (message_received[0]) {
                break;
            }
        }
    }
}