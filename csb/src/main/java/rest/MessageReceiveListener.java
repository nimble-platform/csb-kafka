package rest;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;

import static spark.Spark.port;
import static spark.Spark.post;

/**
 * Created by evgeniyh on 16/03/17.
 */
public class MessageReceiveListener {
    public static void main(String[] args) {
        BasicConfigurator.configure();
        Logger.getRootLogger().removeAllAppenders();
        Logger.getRootLogger().addAppender(new NullAppender());
        System.out.println("Started listening on port 9191");

        port(9191);
        post("/handler/:topic/:message", (request, response) -> {
            String topic = request.params().get(":topic");
            String message = request.params().get(":message");
            String resMessage = String.format("Received message '%s' from topic '%s'", message, topic);
            System.out.println(resMessage);

            return resMessage;
        });
    }
}
