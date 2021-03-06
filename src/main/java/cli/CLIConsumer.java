package cli;

import com.csb.CSBConsumer;
import common.Environment;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;

import java.util.Scanner;
import java.util.Set;

/**
 * Created by evgeniyh on 19/03/17.
 */
public class CLIConsumer {
    private static ArgumentParser parser = getParser();
    private static CSBConsumer consumer;

    public static void main(String[] args) {
        String groupId = (args.length > 0 && args[0] != null) ? args[0] : "cli_consumer";
        Logger.getRootLogger().addAppender(new NullAppender());

        consumer = new CSBConsumer(Environment.PRODUCTION, groupId, null);
        consumer.start();
        System.out.println(String.format("Consumer has started with group id '%s'", groupId));

        Scanner scanner = new Scanner(System.in);
        try {
            while (true) {
                String[] commandArgs = scanner.nextLine().split(" ");
                Namespace namespace = parseArgs(commandArgs);
                if (namespace == null) {
                    continue;
                }
                String command = namespace.getString("command");
                switch (command) {
                    case "list":
                        handleListCommand(namespace);
                        break;
                    case "subscribe":
                        handleSubscribeCommand(consumer, namespace);
                        break;
                    default:
                        System.out.println("Command isn't supported");
                        break;
                }
            }
        } catch (Exception ignored) {
            System.out.println();
            System.out.println("Exiting");
        }
    }

    private static void handleSubscribeCommand(CSBConsumer consumer, Namespace namespace) {
        String topic = namespace.getString("topic_name");
        System.out.println(String.format("Subscribing to topic '%s'", topic));
        consumer.subscribe(topic, m -> System.out.println(String.format("Received '%s' from topic '%s'", m, topic)));
    }

    private static void handleListCommand(Namespace namespace) {
        String whichTopics = namespace.getString("which_topics");
        Set<String> topics;
        switch (whichTopics) {
            case "consumer":
                topics = consumer.getSubscribedTopics();
                if (topics.isEmpty()) {
                    System.out.println("The consumer isn't registered to any topic");
                } else {
                    System.out.println("Printing consumer subscribed topics:");
                    System.out.println("------------------------------------");
                    topics.forEach(System.out::println);
                }
                break;
            case "all":
                topics = consumer.getAvailableTopics();
                if (topics.size() == 1) {
                    System.out.println("There are no available topics");
                } else {
                    System.out.println("Printing all topics available:");
                    System.out.println("------------------------------");
                    topics.forEach(t -> {
                        if (!t.startsWith("__")) {
                            System.out.println(t);
                        }
                    });
                }
                break;
            default:
                System.out.println("Not supported topics list");
                break;
        }
    }

    private static ArgumentParser getParser() {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("CLI Consumer");
        Subparsers subparsers = parser.addSubparsers().title("Consumer Commands").description("Available CLI consumer commands");
        Subparser listTopics = subparsers.addParser("list_topics");
        listTopics.setDefault("command", "list");
        listTopics.addArgument("which_topics").choices("consumer", "all");

        Subparser subscribeParser = subparsers.addParser("subscribe");
        subscribeParser.setDefault("command", "subscribe");
        subscribeParser.addArgument("topic_name").help("The topic to subscribe to");

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
