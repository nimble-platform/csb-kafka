package rest;

import com.csb.CSBConsumer;
import com.csb.topics.CSBMessageHubTopicsHandler;
import common.Environment;
import handlers.IgnoredMessageHandler;
import handlers.MessageHandler;
import handlers.RestMessageHandler;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotSupportedException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
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
        ServerEventHandler serverEventHandler = new ServerEventHandler();
        consumer = new CSBConsumer(Environment.PRODUCTION, DEFAULT_CONSUMER_ID, new CSBMessageHubTopicsHandler(Environment.PRODUCTION, MainRest.API_KEY, MainRest.ADMIN_URL));
        consumer.start();
        logger.info(String.format("Consumer with id '%s' has been started successfully", DEFAULT_CONSUMER_ID));
    }

    @GET
    @Path("/topics")
    public String getConsumerTopics() {
        JSONArray topics = new JSONArray();
        consumer.getAvailableTopics().forEach(topics::add);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("topics", topics);

        return jsonObject.toString();
    }

    //TODO: make logging of only debug to turn off
    //    TODO: make the topic creating inside csb consumer
    @POST
    @Path("/subscribe/{topic}")
    public String subscribeToTopic(@PathParam("topic") String topic,
                                   @QueryParam("handler") String handlerUrl,
                                   @DefaultValue("rest") @QueryParam("type") String handlerType) {
        try {
            MessageHandler mh = getMessageHandler(handlerUrl, handlerType);
            logger.debug(String.format("Subscribing the consumer to topic '%s' and handler url '%s'", topic, handlerUrl));
            consumer.subscribe(topic, mh);
            logger.info(String.format("Successfully subscribed to topic '%s' with handler url '%s'", topic, handlerUrl));
            return "Successfully subscribed to topic";
        } catch (Exception e) {
            logger.error("Failed on subscribe", e);
            return "Failed to subscribe to topic";
        }
    }

    private MessageHandler getMessageHandler(String handlerUrl, String handlerType) {
        switch (handlerType) {
            case "ignore":
                return new IgnoredMessageHandler(true);
            case "rest":
                return new RestMessageHandler(handlerUrl);
            default:
                logger.error(handlerType + " Isn't a supported type");
                throw new NotSupportedException(handlerType + " Isn't a supported type");
        }
    }
}
