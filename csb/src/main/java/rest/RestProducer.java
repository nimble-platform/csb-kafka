package rest;

import com.csb.CSBProducer;
import common.Environment;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Application;

/**
 * Created by evgeniyh on 05/04/17.
 */
@Path("/producer")
public class RestProducer extends Application {
    private final static Logger logger = Logger.getLogger(RestProducer.class);
    private CSBProducer producer = null;
    private String apiKey = null;

    public RestProducer() throws Exception {
        super();
        BasicConfigurator.configure();
        new ServerEventHandler();
//        try {
//            MessageHubCredentials credentials = BluemixEnvironment.getMessageHubCredentials();
//            updateJaasConfiguration(credentials.getUser(), credentials.getPassword());
        producer = new CSBProducer(Environment.PRODUCTION);

        logger.info("Producer has been started successfully");
        apiKey = "uVQSsww0VrYOLO7RxUqrsQBpPtk8FOfp22L537rp4D7AHKhV";
//        } catch (IOException e) {
//            e.printStackTrace();
//            logger.error("Producer failed to start", e);
//        }
    }

    //    TODO : check for existing topic list (at start)
//    TODO: Take admin url from vcap services
    @POST
    @Path("/send/{topic}/{message}")
    public String sendMessage(@PathParam("topic") String topic,
                              @PathParam("message") String message) {
        try {
//            System.out.println("Available topics " + RESTAdmin.listTopics("https://kafka-admin-prod02.messagehub.services.eu-gb.bluemix.net:443", apiKey));
            String restResponse = RESTAdmin.createTopic("https://kafka-admin-prod02.messagehub.services.eu-gb.bluemix.net:443", apiKey, topic);
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
