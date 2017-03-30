package cli;

import com.csb.CSBConsumer;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;

import java.util.Scanner;

/**
 * Created by evgeniyh on 19/03/17.
 */
public class CLIConsumer {
    private static ArgumentParser parser = getParser();

    public static void main(String[] args) {
        String groupId = (args[0] != null) ? args[0] : "cli_consumer";
        Logger.getRootLogger().addAppender(new NullAppender());

        CSBConsumer consumer = new CSBConsumer(groupId);
        consumer.start();
        System.out.println(String.format("Consumer has started with group id '%s'", groupId));

        Scanner scanner = new Scanner(System.in);
        while (true) {
            String[] commandArgs = scanner.nextLine().split(" ");
            Namespace namespace = parseArgs(commandArgs);
            if (namespace == null) {
                continue;
            }
            String topic = namespace.getString("topic_name");
            System.out.println(String.format("Subscribing to topic '%s'", topic));
            consumer.subscribe(topic, m -> System.out.println(String.format("Received '%s' from topic '%s'", m, topic)));
        }
    }

    private static ArgumentParser getParser() {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("CLI Producer").description("Will send data to topic");
        parser.addArgument("command").help("Available consumer commands").choices("subscribe");
        parser.addArgument("topic_name").help("The topic to subscribe to");
        return parser;
    }

    private static Namespace parseArgs(String[] args) {
        try {
            return parser.parseArgs(args);
        } catch (ArgumentParserException ex) {
            parser.handleError(ex);
            return null;
        }
    }
}
