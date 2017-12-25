package rest;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
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
 * Created by evgeniyh on 18/04/17.
 */
public class ServerEventHandler implements ServletContextListener {
    private final static Logger logger = Logger.getLogger(ServerEventHandler.class);
    private final boolean isOnBluemix;
    private final String jaasTemplate;

    public ServerEventHandler() throws Exception {
        InputStream template = ServerEventHandler.class.getClassLoader().getResourceAsStream("jaas.conf.template");
        jaasTemplate = new BufferedReader(new InputStreamReader(template)).lines().parallel().collect(Collectors.joining("\n"));

        String vcapServices = System.getenv("VCAP_SERVICES");
        isOnBluemix = isRunningOnBluemix(vcapServices);
        logEnvironment();

        if (vcapServices == null) {
            logger.error("Vcap services is null - the app isn't bind to message hub");
        } else {
            JsonObject jsonObject = (JsonObject) (new JsonParser().parse(vcapServices));
            JsonArray messageHubJsonArray = jsonObject.getAsJsonArray("messagehub");
            if (messageHubJsonArray == null) {
                logger.error("Couldn't find messagehub key in vcap services env variable");
                return;
            }
            JsonObject credentials = messageHubJsonArray.get(0).getAsJsonObject().get("credentials").getAsJsonObject();
            logger.info("Initialising the messagehub credentials");
            String user = credentials.get("user").getAsString();
            String password = credentials.get("password").getAsString();
            initialiseCredentials(user, password);
        }
    }

    private void logEnvironment() {
        if (isOnBluemix) {
            logger.info("Running on bluemix");
        } else {
            logger.info("Not running on bluemix");
        }
    }

    //    TODO: find how to run this method
    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        System.out.println("Running on created");
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {

    }

    private void initialiseCredentials(String username, String password) throws Exception {
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

    //    TODO: fix to find if running on bluemix - buildpack to return true
    private boolean isRunningOnBluemix(String vcapServices) {
        String userDir = System.getProperty("user.dir");
        File buildpack = new File(userDir + File.separator + ".java-buildpack");

        if (buildpack.exists() && (vcapServices == null)) {
            throw new IllegalStateException("ASSERTION FAILED: buildpack.exists() but VCAP_SERVICES==null");
        }
        return buildpack.exists();
    }
}

