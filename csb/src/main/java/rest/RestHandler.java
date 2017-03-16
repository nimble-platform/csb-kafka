package rest;

import static spark.Spark.put;

/**
 * Created by evgeniyh on 16/03/17.
 */
public class RestHandler {
    public static void main(String[] args) {
        put("/handler/:topic?message=value", (request, response) -> {
            String topic = request.queryParams("topic");


            String test = request.queryParams("$url-greeting");
            String message = request.queryParams("message");
            String msg = String.format("Received message '%s' from topic '%s'", message, topic);
            System.out.println(msg);
            return "message received";
        });
    }
}
