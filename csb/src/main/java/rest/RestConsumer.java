package rest;

import com.csb.CSBConsumer;
import com.csb.MessageHandler;
import common.Environment;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Application;

/**
 * Created by evgeniyh on 05/04/17.
 */
@Path("/consumer")
public class RestConsumer extends Application {
    private final static Logger logger = Logger.getLogger(RestConsumer.class);

    private static String DEFAULT_CONSUMER_ID = "rest_consumer";

    private static CSBConsumer consumer;

    //    TODO: Run method on create of server
    public RestConsumer() throws Exception {
        super();
        BasicConfigurator.configure();
        ServerEventHandler serverEventHandler = new ServerEventHandler();
//        try {
//            MessageHubCredentials credentials = BluemixEnvironment.getMessageHubCredentials();
//            updateJaasConfiguration(credentials.getUser(), credentials.getPassword());
        consumer = new CSBConsumer(Environment.PRODUCTION, DEFAULT_CONSUMER_ID);
        consumer.start();
        logger.info(String.format("Consumer with id '%s' has been started successfully", DEFAULT_CONSUMER_ID));
//        } catch (IOException e) {
//            e.printStackTrace();
//            logger.error("Failed on starting the consumer", e);
//        }
    }

    @GET
    @Path("/topics")
    public String getConsumerTopics() {
        JSONArray topics = new JSONArray();
        consumer.getAvailableTopics().forEach(topics::add);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("Topics", topics);

        return jsonObject.toString();
    }

    @POST
    @Path("/subscribe/{topic}/{handler_url}")
    public String subscribeToTopic(@PathParam("topic") String topic,
                                   @PathParam("handler_url") String handlerUrl) {
        try {
            logger.info(String.format("Subscribing the consumer to topic '%s' and handler url '%s'", topic, handlerUrl));
            MessageHandler mh = new RestMessageHandler(handlerUrl);
            consumer.subscribe(topic, mh);
            logger.info(String.format("Successfully subscribed to topic '%s' with handler url '%s'", topic, handlerUrl));
            return "Successfully subscribed to topic";
        } catch (Exception e) {
            logger.error("Failed on subscribe", e);
            return "Failed to subscribe to topic";
        }
    }
}