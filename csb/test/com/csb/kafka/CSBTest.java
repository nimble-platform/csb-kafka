package com.csb.kafka;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;
import java.util.UUID;

/**
 * Created by evgeniyh on 12/03/17.
 */
public class CSBTest {
    private static final int MAX_TIMEOUT = 40000;
    private static final int DEFAULT_SLEEP = 300;
    private static CSBProducer producer;
    private static CSBConsumer consumer;

    private static String TEST_TOPIC = "test_topic_";

    @Test
    public void testReceiveOneMessage() throws Exception {
        final boolean[] message_received = {false};
        consumer.register(TEST_TOPIC, message -> message_received[0] = true);
//        producer.sendMsgNoWait(TEST_TOPIC, "test_message");
        consumer.start();
        producer.sendMsg(TEST_TOPIC, "test_message" + new Random().nextInt(), false);

        awaitEqualsOrReturn(message_received[0], Boolean.TRUE);
        Assert.assertTrue(message_received[0]);
    }

    @Test(expected = IllegalAccessError.class)
    public void failOnStartingWithoutRegistering() throws Exception {
        CSBConsumer consumer2 = new CSBConsumer("TEST_GROUP");
        consumer2.start();
    }

    @Test
    public void testTwoConsumersReceiveSameMessage() throws Exception {
        CSBConsumer consumer1 = new CSBConsumer("GROUP_1");
        CSBConsumer consumer2 = new CSBConsumer("GROUP_2");

        Random r = new Random();
        String randomNumber = String.valueOf(r.nextInt());
        String RANDOM_TOPIC = TEST_TOPIC + randomNumber;

        final Integer[] counter = new Integer[]{0};
        consumer1.register(RANDOM_TOPIC, message -> {
            if (message.equals(randomNumber)) {
                counter[0] = new Integer(counter[0] + 1);
            }
        });
        consumer2.register(RANDOM_TOPIC, message -> {
            if (message.equals(randomNumber)) {
                counter[0] = new Integer(counter[0] + 1);
            }
        });
        consumer1.start();
        consumer2.start();
        producer.sendMsgNoWait(RANDOM_TOPIC, randomNumber);

        awaitEqualsOrReturn(counter[0], 2);
        Assert.assertSame(2, counter[0]);

        consumer1.close();
        consumer2.close();
    }

    private void awaitEqualsOrReturn(Object a, Object b) throws InterruptedException {
        int waited = 0;
        while (waited < MAX_TIMEOUT) {
            if (a.equals(b)) {
                break;
            } else {
                Thread.sleep(DEFAULT_SLEEP);
                waited += DEFAULT_SLEEP;
            }
        }
    }

    private void sendRandomMessages(int count, String topic) {
        for (int i = 0; i < count; i++) {
            try {
                producer.sendMsg(topic, "test_message" + new Random().nextInt(), false);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail("Exception " + e.getMessage());
            }
        }
    }

    @BeforeClass
    public static void setUp() {
//        TEST_TOPIC = "test_topic_" + new Data();
        TEST_TOPIC = "test_topic_222";
        producer = new CSBProducer();
        consumer = new CSBConsumer(UUID.randomUUID().toString());
    }

    @AfterClass
    public static void cleanUp() {
        producer.close();
        consumer.close();
    }
}