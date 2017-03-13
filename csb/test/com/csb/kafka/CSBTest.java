package com.csb.kafka;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;

/**
 * Created by evgeniyh on 12/03/17.
 */
public class CSBTest {
    private static final int MAX_TIMEOUT = 3000;
    private static final int DEFAULT_SLEEP = 300;

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

        int waited = 0;
        while (waited < MAX_TIMEOUT) {
            if (!message_received[0]) {
                Thread.sleep(DEFAULT_SLEEP);
                waited += DEFAULT_SLEEP;
            }
        }
        Assert.assertTrue(message_received[0]);
    }

    @Test(expected = IllegalAccessError.class)
    public void failOnStartingWithoutRegistering() throws Exception {
        CSBConsumer consumer = new CSBConsumer();
        consumer.start();
    }

    @Test
    public void testTwoConsumersReceiveSameMessage() throws Exception {
        CSBConsumer consumer1 = new CSBConsumer();
        CSBConsumer consumer2 = new CSBConsumer();
        CSBProducer producer = new CSBProducer();

        Random r = new Random();
        String number = String.valueOf(r.nextInt());
        producer.sendMsg(TEST_TOPIC, number);

        final Integer[] counter = new Integer[]{0};
        consumer1.register(TEST_TOPIC, message -> {
            if (message.equals(number)) {
                counter[0]++;
            }
        });
        consumer2.register(TEST_TOPIC, message -> {
            if (message.equals(number)) {
                counter[0]++;
            }
        });
        consumer1.start();
        consumer2.start();

        int waited = 0;
        while (waited < MAX_TIMEOUT) {
            if (counter[0] != 2) {
                Thread.sleep(DEFAULT_SLEEP);
                waited += DEFAULT_SLEEP;
            }
        }
        Assert.assertTrue(counter[0] == 2);
    }

    @BeforeClass
    public static void setUp() {

    }

    @AfterClass
    public static void cleanUp() {

    }
}