package rest;

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
@WebListener
@Singleton
public class MainRest extends Application implements ServletContextListener {
    private final static Logger logger = Logger.getLogger(MainRest.class);
    //    private final boolean isOnBluemix;

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

    public MainRest() throws Exception {
        logger.info("in build main");

        InputStream template = MainRest.class.getClassLoader().getResourceAsStream("jaas.conf.template");
        String jaasTemplate = new BufferedReader(new InputStreamReader(template)).lines().parallel().collect(Collectors.joining("\n"));

//        String vcapServices = System.getenv("VCAP_SERVICES");
//        isOnBluemix = isRunningOnBluemix(vcapServices);
//        logEnvironment();

//        if (vcapServices == null) {
//            logger.error("Vcap services is null - the app isn't bind to message hub");
//        } else {
//            JsonObject jsonObject = (JsonObject) (new JsonParser().parse(vcapServices));
//            JsonArray messageHubJsonArray = jsonObject.getAsJsonArray("messagehub");
//            if (messageHubJsonArray == null) {
//                logger.error("Couldn't find messagehub key in vcap services env variable");
//                return;
//            }
//            JsonObject credentials = messageHubJsonArray.get(0).getAsJsonObject().get("credentials").getAsJsonObject();
//            logger.info("Initialising the messagehub credentials");
//            String user = credentials.get("user").getAsString();
//            String password = credentials.get("password").getAsString();
            initialiseCredentials(jaasTemplate, API_KEY.substring(0,17), API_KEY.substring(17));
//        }
    }

//    private void logEnvironment() {
//        if (isOnBluemix) {
//            logger.info("Running on bluemix");
//        } else {
//            logger.info("Not running on bluemix");
//        }
//    }

    @Override
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

//    //    TODO: fix to find if running on bluemix - buildpack to return true
//    private boolean isRunningOnBluemix(String vcapServices) {
//        String userDir = System.getProperty("user.dir");
//        File buildpack = new File(userDir + File.separator + ".java-buildpack");
//
//        if (buildpack.exists() && (vcapServices == null)) {
//            throw new IllegalStateException("ASSERTION FAILED: buildpack.exists() but VCAP_SERVICES==null");
//        }
//        return buildpack.exists();
//    }
}
