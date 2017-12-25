package com.csb.topics;

import java.util.Set;

/**
 * Created by evgeniyh on 30/04/17.
 */
public interface CSBTopicsHandler {
    boolean isTopicsExists(String topic);

    void createTopicSync(String topic) throws Exception;

    Set<String> getTopics();
}
