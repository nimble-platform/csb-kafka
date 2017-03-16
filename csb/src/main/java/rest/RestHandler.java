package rest;

import static spark.Spark.port;
import static spark.Spark.put;

/**
 * Created by evgeniyh on 16/03/17.
 */
public class RestHandler {
    public static void main(String[] args) {
        port(9191);
        put("/handler/:topic/:message", (request, response) -> {
            String topic = request.params().get(":topic");
            String message = request.params().get(":message");

            return String.format("Received message '%s' from topic '%s'", message, topic);
        });
    }
}
