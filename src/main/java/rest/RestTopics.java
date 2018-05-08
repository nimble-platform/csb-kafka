package rest;

import com.csb.topics.CSBMessageHubTopicsHandler;
import com.csb.topics.CSBTopicsHandler;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import common.Environment;
import org.apache.log4j.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Application;

@Path("/topics")
public class RestTopics extends Application {
    private final static Logger logger = Logger.getLogger(RestTopics.class);

    private static CSBTopicsHandler topicsHandler;

    //    TODO: replace with onServerCreate
    static {
        logger.info("Running the setup");
        topicsHandler = new CSBMessageHubTopicsHandler(Environment.PRODUCTION, MainRest.API_KEY, MainRest.ADMIN_URL);
    }

    @POST
    @Path("/create/{topic_name}")
    public static String createTopic(@PathParam("topic_name") String topicName) {
        logger.info(String.format("Trying to create topic named '%s'", topicName));
        if (topicsHandler.isTopicsExists(topicName)) {
            return String.format("Topic '%s' already exists - failed to create", topicName);
        }
        try {
            topicsHandler.createTopicSync(topicName);
            return String.format("Topic '%s' was created successfully", topicName);
        } catch (Exception e) {
            logger.error(String.format("Failed to create topic '%s'", e));
            return "Failed to create topic - " + e.getMessage();
        }
    }

    @GET
    public String getTopics() {
        JsonArray topics = new JsonArray();
        topicsHandler.getTopics().forEach(topics::add);
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("Topics", topics);

        return jsonObject.toString();
    }
}
