package com.csb.topics;

import common.Environment;
import common.PropertiesLoader;
import org.apache.log4j.Logger;
import rest.CreateTopicConfig;
import rest.CreateTopicParameters;
import rest.RESTRequest;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

//TODO: load the list of topics on start
public class CSBMessageHubTopicsHandler implements CSBTopicsHandler {
    private final static Logger logger = Logger.getLogger(CSBMessageHubTopicsHandler.class);

    private static final long _24H_IN_MILLISECONDS = 3600000L * 24;

    private final static Object topicsSync = new Object();
    final private int partitions;

    private final HashSet<String> topics = new HashSet<>();
    private final String apiKey;
    private final String adminUrl;

    public CSBMessageHubTopicsHandler(Environment environment, String apiKey, String adminUrl) {
        logger.info(String.format("The admin url is set to '%s'", adminUrl));
        this.apiKey = apiKey;
        this.adminUrl = adminUrl;

        Properties prop = PropertiesLoader.loadProperties(PropertiesLoader.TOPIC_CREATOR, environment);

        partitions = Integer.parseInt(prop.getProperty("kafka.partitions"));

        RESTRequest restApi = new RESTRequest(adminUrl, apiKey);
        try {
            String topics = restApi.get("/admin/topics", false);
            System.out.println(topics);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean isTopicsExists(String topic) {
        synchronized (topicsSync) {
            return topics.contains(topic);
        }
    }

    @Override
    public void createTopicSync(String topic) throws Exception {
        logger.debug(String.format("Trying to create topic '%s'", topic));

        try {
            RESTRequest restApi = new RESTRequest(adminUrl, apiKey);

            // Create a topic, ignore a 422 response - this means that the
            // topic name already exists.
            String postResult = restApi.post("/admin/topics",
                    new CreateTopicParameters(topic, partitions,
                            new CreateTopicConfig(_24H_IN_MILLISECONDS)).toString(),
                    new int[]{422});

            logger.info(String.format("Topic named '%s' was created with POST result - '%s'", topic, postResult));
            topics.add(topic);
        } catch (Exception e) {
            logger.error(String.format("Exception on creating topic '%s' ", topic), e);
            throw e;
        }
    }

    @Override
    public Set<String> getTopics() {
        synchronized (topicsSync) {
            return new HashSet<>(topics);
        }
    }
}
