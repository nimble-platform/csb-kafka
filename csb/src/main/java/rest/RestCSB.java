package rest;

import com.csb.CSBConsumer;
import com.csb.CSBProducer;

import static spark.Spark.get;

/**
 * Created by evgeniyh on 15/03/17.
 */
public class RestCSB implements AutoCloseable {
    private final CSBConsumer consumer;
    private final CSBProducer producer;

    public RestCSB() {
        consumer = new CSBConsumer("rest_consumer");
        producer = new CSBProducer();
        consumer.start();
    }

    //    TODO: add mapping for clients
    public void start() {
        get("/consumer/register/:topic/:handler", (request, response) -> {
            String topic = request.params().get(":topic");
            String handlerUrl = request.params().get(":handler");
            registerToTopic(topic, handlerUrl);

            String value1 = request.queryParams("asd");

            return String.format("Successfully Registered to topic '%s'", "SD");
        });
    }

    private void registerToTopic(String topic, String handlerUrl) {
        consumer.register(topic, new RestMessageHandler(handlerUrl));
    }

    @Override
    public void close() throws Exception {
        producer.close();
        consumer.close();
    }
}
