package rest;

import static spark.Spark.get;

/**
 * Created by evgeniyh on 15/03/17.
 */
public class RestCSB {
    public RestCSB() {
    }

    public void start() {
        get("/users/:id", (request, response) -> "User: username=test, email=test@test.net");

    }
}
