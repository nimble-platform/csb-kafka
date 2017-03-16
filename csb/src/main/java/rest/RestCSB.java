package rest;

import com.csb.CSBConsumer;
import com.csb.CSBProducer;

import java.util.Map;

import static spark.Spark.put;

/**
 * Created by evgeniyh on 15/03/17.
 */
public class RestCSB implements AutoCloseable {
    private final CSBConsumer consumer;

    public RestCSB() {
        consumer = new CSBConsumer("rest_consumer");
        consumer.start();
    }

    //    TODO: add mapping for clients
    public void start() {
        put("/consumer/register/:topic", (request, response) -> {
            Map<String, String> params = request.params();
            String topic = params.get(":topic");
            String handlerUrl = params.get(":handler");
            return registerToTopic(topic, handlerUrl);
        });

        put("/consumer/register", (request, response) -> {
            String topic = request.queryParams("topic");
            String handlerUrl = request.queryParams("handler");

            if (topic == null || handlerUrl == null) {
                return "Failed to parse topic and message handler";
            } else {
                return registerToTopic(topic, handlerUrl);
            }
        });

        put("/producer/send/:topic/:message", (request, response) -> {
            Map<String, String> params = request.params();
            String topic = params.get(":topic");
            String message = params.get(":message");
            return sendMessage(topic, message);
        });

        put("/producer/send", (request, response) -> {
            String topic = request.queryParams("topic");
            String message = request.queryParams("message");

            if (topic == null || message == null) {
                return "Failed to parse topic and message";
            } else {
                return sendMessage(topic, message);
            }
        });
    }

    //    TODO: fix issue when using the same producer
    private String sendMessage(String topic, String message) {
        try (CSBProducer producer = new CSBProducer()) {
            producer.sendMsgNoWait(topic, message);
            return "Message was sent successfully";
        } catch (Exception e) {
            e.printStackTrace();
            return "Failed to send message " + e.getMessage();
        }
    }

    private String registerToTopic(String topic, String handlerUrl) {
        consumer.register(topic, new RestMessageHandler(handlerUrl));
        return String.format("Successfully Registered to topic '%s'", "SD");
    }

    @Override
    public void close() throws Exception {
        consumer.close();
    }
}
