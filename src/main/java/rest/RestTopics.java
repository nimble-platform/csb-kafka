package rest;

import com.csb.topics.CSBMessageHubTopicsHandler;
import com.csb.topics.CSBTopicsHandler;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
        String vcapServices = System.getenv("VCAP_SERVICES");
        if (vcapServices == null) {
            logger.error("Vcap services is null - the app isn't bind to any service");
        } else {
            JsonObject jsonObject = (JsonObject) (new JsonParser().parse(vcapServices));
            JsonArray messageHubJsonArray = jsonObject.getAsJsonArray("messagehub");
            if (messageHubJsonArray == null) {
                logger.error("Couldn't find messagehub key in vcap services env variable");
            } else {
                topicsHandler = getTopicsHandler(messageHubJsonArray);
            }
        }
    }

    private static CSBTopicsHandler getTopicsHandler(JsonArray messageHubJsonArray) {
        JsonObject credentials = messageHubJsonArray.get(0).getAsJsonObject().get("credentials").getAsJsonObject();
        logger.debug("Retrieving admin url and api-key");
        String restAdminUrl = credentials.get("kafka_admin_url").getAsString();
        String apiKey = credentials.get("api_key").getAsString();

        if (apiKey == null || restAdminUrl == null) {
            logger.error("Failed to initialise admin url and api key");
            throw new RuntimeException("Unable to set the topics handler");
        } else {
            logger.debug("Admin url and api-key were set successfully");
        }
        return new CSBMessageHubTopicsHandler(Environment.PRODUCTION, apiKey, restAdminUrl);
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
    @Path("/")
    public String getTopics() throws Exception {
        JsonArray topics = new JsonArray();
        topicsHandler.getTopics().forEach(topics::add);
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("Topics", topics);

        return jsonObject.toString();
    }
}
