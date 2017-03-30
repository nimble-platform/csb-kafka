package com.csb;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
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

    private static String TEST_TOPIC_PREFIX = "test_topic_";
    private static String TEST_MESSAGE_PREFIX = "test_message_";
    private static Random randomGenerator;

    @Ignore
    @Test
    public void testReceiveOneMessage() throws Exception {
        String testMessage = "This is a test message " + new Random().nextInt();
        MessageCounter mc = new MessageCounter(testMessage);
        producer.sendMsgNoWait(TEST_TOPIC_PREFIX, testMessage);

        consumer.subscribe(TEST_TOPIC_PREFIX, mc);
        consumer.start();

        awaitEqualsOrReturn(mc, 1);
        Assert.assertTrue(mc.getCounter() == 1);
    }

    @Ignore
    @Test(expected = IllegalAccessError.class)
    public void failOnStartingWithoutRegistering() throws Exception {
        CSBConsumer consumer2 = new CSBConsumer("TEST_GROUP");
        consumer2.start();
    }

    @Ignore
    @Test
    public void testTwoConsumersReceiveSameMessage() throws Exception {
        CSBConsumer consumer1 = new CSBConsumer("GROUP_1");
        CSBConsumer consumer2 = new CSBConsumer("GROUP_2");

        String randomNumber = String.valueOf(randomGenerator.nextInt());
        String RANDOM_TOPIC = TEST_TOPIC_PREFIX + randomNumber;
        producer.sendMsgNoWait(RANDOM_TOPIC, randomNumber);

        MessageCounter messageCounter = new MessageCounter(randomNumber);

        consumer1.subscribe(RANDOM_TOPIC, messageCounter);
        consumer2.subscribe(RANDOM_TOPIC, messageCounter);
        consumer1.start();
        consumer2.start();

        awaitEqualsOrReturn(messageCounter, 2);
        Assert.assertTrue(messageCounter.getCounter() == 2);

        consumer1.close();
        consumer2.close();
    }


    private void awaitEqualsOrReturn(MessageCounter mc, int expectedCount) throws InterruptedException {
        int waited = 0;
        while (waited < MAX_TIMEOUT) {
            if (mc.getCounter() == expectedCount) {
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
                producer.sendMsgInBulk(topic, "test_message" + new Random().nextInt());
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail("Exception " + e.getMessage());
            }
        }
    }

    @BeforeClass
    public static void setUp() {
        TEST_TOPIC_PREFIX = "test_topic_" + (new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(new Date()));
        producer = new CSBProducer();
        randomGenerator = new Random();
        consumer = new CSBConsumer(UUID.randomUUID().toString());
    }

    @AfterClass
    public static void cleanUp() {
        producer.close();
        if (consumer.isActivated()) {
            consumer.close();
        }
    }

    private class MessageCounter implements MessageHandler {
        private final String expectedMsg;
        private int counter = 0;

        public MessageCounter(String expectedMsg) {
            this.expectedMsg = expectedMsg;
        }

        @Override
        synchronized public void handle(String message) {
            if (message.equals(expectedMsg)) {
                counter++;
            }
        }

        public int getCounter() {
            return counter;
        }
    }
}