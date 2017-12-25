package cli;

import com.csb.CSBProducer;
import common.Environment;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;

/**
 * Created by evgeniyh on 19/03/17.
 */
public class CLIProducer {
    public static void main(String[] args) {
        Logger.getRootLogger().addAppender(new NullAppender());
        Namespace namespace = parseArgs(args);
        if (namespace == null) {
            return;
        }

        String topic = namespace.getString("topic_name");
        String message = namespace.getString("message");

        CSBProducer producer = new CSBProducer(Environment.PRODUCTION);
        System.out.println(String.format("Trying to send '%s' to topic '%s'", message, topic));
        if (producer.sendMsgNoWait(topic, message)) {
            System.out.println("Data was sent successfully");
        } else {
            System.out.println("Failed to send data");
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("CLI Producer").description("Will send data to topic");
        parser.addArgument("topic_name").help("The name of the target topic");
        parser.addArgument("message").help("The data to be send");

        try {
            return parser.parseArgs(args);
        } catch (ArgumentParserException ex) {
            parser.handleError(ex);
            return null;
        }
    }
}
