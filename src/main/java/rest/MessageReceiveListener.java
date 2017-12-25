package rest;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by evgeniyh on 16/03/17.
 */
public class MessageReceiveListener {
    public static void main(String[] args) {
        BasicConfigurator.configure();
        Logger.getRootLogger().removeAllAppenders();
        Logger.getRootLogger().addAppender(new NullAppender());
//        System.out.println("Started listening on port 9191");
//
//        port(9191);
//        post("/handler/:topic/:message", (request, response) -> {
//            String topic = request.params().get(":topic");
//            String message = request.params().get(":message");
//            String resMessage = String.format("Received message '%s' from topic '%s'", message, topic);
//            System.out.println(resMessage);
//
//            return resMessage;
//        });
        try {

            HttpClient httpclient = HttpClients.createDefault();

            HttpGet httpGet = new HttpGet("http://object-store-app.eu-gb.mybluemix.net/objectStorage?file=friday-deploy.jpg");
            HttpResponse getResponse = httpclient.execute(httpGet);
            HttpEntity getEntity = getResponse.getEntity();

            String filePath = "friday-deploy.jpg";
            try (InputStream inStream = getEntity.getContent();
                 FileOutputStream fos = new FileOutputStream(new File(filePath))) {
                int inByte;
                while ((inByte = inStream.read()) != -1)
                    fos.write(inByte);
            }
            System.out.println("File download complete");

            HttpPost httppost = new HttpPost("http://object-store-app.eu-gb.mybluemix.net/objectStorage?file=friday-deploy.jpg");
            httppost.setHeader(HttpHeaders.CONTENT_TYPE, "image/jpeg");

            Path path = Paths.get("C:\\Users\\evgeniyh\\Desktop\\friday-deploy.jpg");
            byte[] data = Files.readAllBytes(path);
            // get DataBufferBytes from Raster
            httppost.setEntity(new ByteArrayEntity(data));
            //Execute and get the response.
            HttpResponse response = httpclient.execute(httppost);
            HttpEntity entity = response.getEntity();

            if (entity != null) {
                try (InputStream inStream = entity.getContent()) {
                    // The output should be -1 (empty response)
                    if (inStream.read() == -1) {
                        System.out.println("SUCCESS - file was sent");
                    } else {
                        System.out.println("ERROR - file wasn't sent");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
