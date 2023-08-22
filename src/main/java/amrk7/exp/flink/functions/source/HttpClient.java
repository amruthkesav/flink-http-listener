package amrk7.exp.flink.functions.source;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.api.ACLProvider;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.ACL;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

public class HttpClient {

    public static void main(String[] args) throws Exception {

        ZkRegistry zkRegistry = new ZkRegistry(
                "localhost:2181",
                new ACLProvider() {
                    @Override
                    public List<ACL> getDefaultAcl() {
                        return null;
                    }

                    @Override
                    public List<ACL> getAclForPath(String s) {
                        return null;
                    }
                },
                10000,
                10000,
                30
        );

        List<String> servers = zkRegistry.discoverInstances();
        if (servers.size() == 0) {
            throw new RuntimeException("No servers available");
        }

        String serverDescriptor = zkRegistry.discoverInstances().get(0);
        String serverUrl = "http://" + serverDescriptor.split(";")[0];
        String data = "Sample data to send to server";


        try {
            URL url = new URL(serverUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);

            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = data.getBytes("utf-8");
                for (int i=0; i < 10; i++)
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