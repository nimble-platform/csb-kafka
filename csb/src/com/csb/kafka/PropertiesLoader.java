package com.csb.kafka;

import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by evgeniyh on 09/03/17.
 */
public class PropertiesLoader {
    final private static Logger logger = Logger.getLogger("basic");

    public static String CONSUMER_DEV = "consumer_dev.properties";
    public static String PRODUCER_DEV = "producer_dev.properties";

    public static Properties loadProperties(String file) {
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
            return null;
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception e) {
                }
            }
        }
    }
}
