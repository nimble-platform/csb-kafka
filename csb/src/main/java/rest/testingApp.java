package rest;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;

/**
 * Created by evgeniyh on 05/04/17.
 */
@ApplicationPath("/")
@Path("/")
public class testingApp extends Application {

    @GET
    @Path("/a")
    public String a() {
        return "a";
    }

    @GET
    @Path("/b")
    public String b() {
        return "another_test";
    }
}
