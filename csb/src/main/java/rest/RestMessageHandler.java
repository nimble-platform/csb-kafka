package rest;

import com.csb.MessageHandler;

import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

/**
 * Created by evgeniyh on 16/03/17.
 */
public class RestMessageHandler implements MessageHandler {
    private final String handlerUrl;

    public RestMessageHandler(String handlerUrl) {
        this.handlerUrl = handlerUrl;
    }

    @Override
    public void handle(String message) {
        try {
            URL url = new URL(handlerUrl + URLEncoder.encode(message, "UTF-8"));
            HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
            httpCon.setDoOutput(true);
            httpCon.setRequestMethod("PUT");
            OutputStreamWriter out = new OutputStreamWriter(httpCon.getOutputStream());
            out.write("Resource content");
            out.close();
            httpCon.getInputStream();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
