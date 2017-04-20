package rest;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;

/**
 * Created by evgeniyh on 16/03/17.
 */
@ApplicationPath("/")
@Path("/")
public class MainRest extends Application {

    @GET
    @Path("/")
    public String hello() {
        return "Hello from CSB-Service";
    }
}
