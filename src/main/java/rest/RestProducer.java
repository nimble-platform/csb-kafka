package rest;

import com.csb.CSBProducer;
import common.Environment;
import org.apache.log4j.Logger;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;

/**
 * Created by evgeniyh on 05/04/17.
 */
@Path("/producer")
public class RestProducer extends Application {
    private final static Logger logger = Logger.getLogger(RestProducer.class);
    private CSBProducer producer;

    public RestProducer() {
        super();
        producer = new CSBProducer(Environment.PRODUCTION);
        logger.debug("Producer has been started successfully");
    }

    //    TODO : check for existing topic list (at start)
    @POST
    @Path("/send/{topic}")
    @Consumes(MediaType.TEXT_PLAIN)
    public String sendMessage(@PathParam("topic") String topic, @QueryParam("message") String message, String body) {
        // if message is sent as content then it will receive in the body

        if (message == null || message.isEmpty()) {
            message = body;
        }
        try {
//            System.out.println("Available topics " + RestTopics.listTopics("https://kafka-admin-prod02.messagehub.services.eu-gb.bluemix.net:443", apiKey));
            String restResponse = RestTopics.createTopic(topic);
            logger.info("Created topic " + topic + " - " + restResponse);
        } catch (Exception e) {
            logger.info("Error on creating topic " + e.getMessage());
        }
        logger.info(String.format("Sending '%s' to topic '%s'", message, topic));
        if (producer.sendMsgNoWait(topic, message)) {
            logger.info(String.format("Successfully sent '%s' to topic '%s'", message, topic));
            return "sending message was successful";
        } else {
            logger.info(String.format("Failed to send '%s' to topic '%s'", message, topic));
            return "Failed to send message";
        }
    }
}
