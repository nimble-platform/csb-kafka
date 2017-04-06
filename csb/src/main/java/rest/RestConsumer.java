package rest;

import com.csb.CSBConsumer;
import common.Environment;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;

/**
 * Created by evgeniyh on 05/04/17.
 */
@ApplicationPath("/")
@Path("/consumer")
public class RestConsumer extends Application {
    private static String DEFAULT_CONSUMER_ID = "rest_consumer";

    private static CSBConsumer consumer;

    public RestConsumer() {
        super();
        consumer = new CSBConsumer(Environment.PRODUCTION, DEFAULT_CONSUMER_ID);
        consumer.start();
    }


    @POST
    @Path("/subscribe")
    public String subscribeToTopic() {
        System.out.println("Consumer is started = " + consumer.isStarted());
        return "Successfully subscribed to topic";
    }
}
