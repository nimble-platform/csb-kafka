import static spark.Spark.get;


/**
 * Created by evgeniyh on 15/03/17.
 */
public class RestConsumer {
    public static void main(String[] args) {

        get("/users/:id", (request, response) -> "User: username=test, email=test@test.net");
    }
}
