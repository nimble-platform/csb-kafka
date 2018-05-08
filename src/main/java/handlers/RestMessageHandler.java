package handlers;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;
import rest.MainRest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

/**
 * Created by evgeniyh on 16/03/17.
 */
public class RestMessageHandler implements MessageHandler {
    private final static Logger logger = Logger.getLogger(RestMessageHandler.class);

    private final String handlerUrl;

    public RestMessageHandler(String handlerUrl) {
        this.handlerUrl = handlerUrl;
    }

    @Override
    public void handle(String message) {
        logger.info(String.format("Sending message '%s' to url '%s'", message, handlerUrl));

        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpPost httpPost = new HttpPost(handlerUrl);
            HttpEntity entity = new ByteArrayEntity(message.getBytes("UTF-8"));
            httpPost.setEntity(entity);

            HttpResponse response = httpclient.execute(httpPost);
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new IllegalAccessException("Failed to send the message");
            } else {
                String res = inputStreamToString(response.getEntity().getContent());
                logger.info("Successfully sent message to the endpoint : " + res);
            }
        } catch (Exception e) {
            logger.error(e);
        }
    }

    private String inputStreamToString(InputStream stream) throws IOException {
        StringWriter writer = new StringWriter();
        IOUtils.copy(stream, writer, "UTF-8");
        return writer.toString();
    }

}
