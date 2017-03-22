package cli;

/**
 * Created by evgeniyh on 19/03/17.
 */
public class CLIProducer {
    private static String usageMessage = "Wrong command format: use \nsend --topic 'my_topic' --message 'my_message'";

    public static void main(String[] args) {
        if (args.length < 5) {
            System.out.println(usageMessage);
        }
        if (!(args[2].equals("send") && args[3].equals("--topic") && args[5].equals("--message"))) {
            System.out.println(usageMessage);
        }
    }
}
