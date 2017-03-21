package rest;

import static spark.Spark.port;
import static spark.Spark.put;

/**
 * Created by evgeniyh on 16/03/17.
 */
public class MessageReceiveListener {
    public static void main(String[] args) {
        System.out.println("Started listening on port 9191");

        port(9191);
        put("/handler/:topic/:message", (request, response) -> {
            String topic = request.params().get(":topic");
            String message = request.params().get(":message");
            String resMessage = String.format("Received message '%s' from topic '%s'", message, topic);
            System.out.println(resMessage);

            return resMessage;
        });
    }
}
