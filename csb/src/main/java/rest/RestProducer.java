package rest;

import com.csb.CSBProducer;
import common.Environment;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;

/**
 * Created by evgeniyh on 05/04/17.
 */
@Path("/producer")
public class RestProducer extends Application {
    private final CSBProducer producer;

    public RestProducer() {
        super();
        producer = new CSBProducer(Environment.PRODUCTION);
    }

    @POST
    @Path("/send")
    public String sendMessage() {
        return "sending message was successful";
    }
}
