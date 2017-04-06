package cli;

import com.csb.CSBTopicCreator;
import common.Environment;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;

/**
 * Created by evgeniyh on 29/03/17.
 */
public class CLICreator {
    public static void main(String[] args) {
        Logger.getRootLogger().addAppender(new NullAppender());
        Namespace namespace = parseArgs(args);
        if (namespace == null) {
            return;
        }
        String topicName = namespace.get("topic_name");
        CSBTopicCreator topicCreator = new CSBTopicCreator(Environment.PRODUCTION);

        System.out.println(String.format("Trying to create topic named '%s'", topicName));
        if (topicCreator.createTopicSync(topicName)) {
            System.out.println("Topic was created successfully");
        } else {
            System.out.println("Couldn't create the topic");
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("CLI Creator").description("Will create a topic in kafka");
        parser.addArgument("topic_name").help("The name for the topic to create");

        try {
            return parser.parseArgs(args);
        } catch (ArgumentParserException ex) {
            parser.handleError(ex);
            return null;
        }
    }
}
