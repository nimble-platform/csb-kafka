package handlers;

/**
 * Created by evgeniyh on 20/04/17.
 */
public class IgnoredMessageHandler implements MessageHandler {
    private final boolean print;

    public IgnoredMessageHandler(boolean print) {
        this.print = print;
    }

    @Override
    public void handle(String message) {
        if (print) {
            System.out.println("Received message " + message);
        }
    }
}
