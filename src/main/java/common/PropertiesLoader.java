package common;

import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by evgeniyh on 09/03/17.
 */
public class PropertiesLoader {
    final private static Logger logger = Logger.getLogger(PropertiesLoader.class);

    public static String CONSUMER = "consumer";
    public static String PRODUCER = "producer";
    public static String TOPIC_CREATOR = "creator";

    public static Properties loadProperties(String clientType, Environment environment) {
        String file = String.format("%s_%s.properties", clientType, environment);
        InputStream inputStream = PropertiesLoader.class.getClassLoader().getResourceAsStream(file);
        try {
            if (inputStream == null) {
                throw new FileNotFoundException("property file '" + file + "' not found in the classpath");
            }
            Properties prop = new Properties();
            prop.load(inputStream);

            return prop;
        } catch (Exception ex) {
            logger.info(String.format("Exception '%s' on loading properties file '%s'", ex.getMessage(), file));
            throw new RuntimeException("Unable to load properties");
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception ignored) {
                }
            }
        }
    }
}
