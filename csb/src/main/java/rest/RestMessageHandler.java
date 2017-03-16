package rest;

import com.csb.MessageHandler;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
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
            InputStream stream = httpCon.getInputStream();
            BufferedReader buf = new BufferedReader(new InputStreamReader(stream, "UTF-8"));


        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
