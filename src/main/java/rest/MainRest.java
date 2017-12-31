package rest;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.log4j.Logger;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;

/**
 * Created by evgeniyh on 16/03/17.
 */
@ApplicationPath("/")
@Path("/")
public class MainRest extends Application {
    private final static Logger logger = Logger.getLogger(MainRest.class);

    public static final String API_KEY;
    public static final String ADMIN_URL;

    static {
        String credentials = System.getenv("MESSAGE_HUB_CREDENTIALS");
        if (credentials == null) {
            logger.error("Failed to get message hub credentials - exiting");
            System.exit(1);
        }
        logger.info(credentials);
        JsonObject jsonObject = (JsonObject) (new JsonParser().parse(credentials));
        ADMIN_URL = jsonObject.get("kafka_admin_url").getAsString();
        API_KEY = jsonObject.get("api_key").getAsString();

        if (API_KEY == null || ADMIN_URL == null) {
            logger.error("Failed to initialise admin url and api key");
            throw new RuntimeException("Unable to set the topics handler");
        } else {
            logger.debug("Admin url and api-key were set successfully");
        }
    }

    @GET
    @Path("/")
    public String hello() {
        return "Hello from CSB-Service";
    }
}
