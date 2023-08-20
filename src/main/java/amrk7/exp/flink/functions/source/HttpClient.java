package amrk7.exp.flink.functions.source;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpClient {

    public static void main(String[] args) {
        String serverUrl = "http://localhost:8080"; // Change to your server URL
        String data = "Sample data to send to server";

        try {
            URL url = new URL(serverUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);

            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = data.getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            int responseCode = connection.getResponseCode();
            System.out.println("Response Code: " + responseCode);

            // You can also read the response from the server if needed
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}