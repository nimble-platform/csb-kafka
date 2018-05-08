package rest;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import javax.inject.Singleton;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.stream.Collectors;

/**
 * Created by evgeniyh on 16/03/17.
 */
@ApplicationPath("/")
@Path("/")
@Singleton
public class MainRest extends Application implements ServletContextListener {
    private final static Logger logger = Logger.getLogger(MainRest.class);

    public static final String API_KEY;
    public static final String ADMIN_URL;

    static {
        String credentials = System.getenv("MESSAGE_HUB_CREDENTIALS");
        if (credentials == null) {
            logger.error("Failed to get message hub credentials - exiting");
            System.exit(1);
        }
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
    public String hello() {
        return "Hello from CSB-Service";
    }

    public MainRest() throws Exception {
        logger.info("in build main");

        InputStream template = MainRest.class.getClassLoader().getResourceAsStream("jaas.conf.template");
        String jaasTemplate = new BufferedReader(new InputStreamReader(template)).lines().parallel().collect(Collectors.joining("\n"));
        initialiseCredentials(jaasTemplate, API_KEY.substring(0,17), API_KEY.substring(17));
    }

    public void contextInitialized(ServletContextEvent servletContextEvent) {
        System.out.println("Running on created");
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        logger.info("Shutting down");
    }

    private void initialiseCredentials(String jaasTemplate, String username, String password) throws Exception {
        final String JAAS_CONFIG_PROPERTY = "java.security.auth.login.config";
        String jaasConfPath = System.getProperty("java.io.tmpdir") + File.separator + "jaas.conf";
        System.setProperty(JAAS_CONFIG_PROPERTY, jaasConfPath);

        try (OutputStream jaasOutStream = new FileOutputStream(jaasConfPath, false)) {
            String fileContents = jaasTemplate
                    .replace("$USERNAME", username)
                    .replace("$PASSWORD", password);

            jaasOutStream.write(fileContents.getBytes(Charset.forName("UTF-8")));
            logger.info("Successfully updated the credentials");
        } catch (final IOException e) {
            logger.log(Level.ERROR, "Failed accessing to JAAS config file", e);
            throw e;
        }
    }
}
