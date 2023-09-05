package amrk7.exp.flink.functions.source;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;

import com.uber.m3.tally.Buckets;
import com.uber.m3.tally.Capabilities;
import com.uber.m3.tally.StatsReporter;
import com.uber.m3.util.Duration;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.api.ACLProvider;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A StatsReporter implementation which reports various types of metrics to an HTTP server.
 * This class implements the Tally StatsReporter, allowing it to report metrics such as counters to the server.
 *
 * Metrics can be reported with different types including COUNTER, GAUGE, TIMER, and HISTOGRAM.
 *
 * The class prepares metric payloads, sends HTTP POST requests to the server, and manages the server's URL based on
 * the server discovery through ZooKeeper.
 *
 * Usage Example:
 * ```
 * HttpMetricsReporter httpMetricsReporter = new HttpMetricsReporter("hp-data-observability-XYZ");
 * httpMetricsReporter.reportCounter("metricName", metricMap, metricValue);
 * ```
 */

public class HttpMetricsReporter implements StatsReporter, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(HttpSourceFunction.class);

    private final String topicName;
    private final String serverAddress;

    public enum MetricType {
        COUNTER,
        GAUGE,
        TIMER,
        HISTOGRAM
    }

    public HttpMetricsReporter(String topicName) {
        this.topicName = topicName;
        serverAddress = getServerUrlWithZK();
    }

    private String preparePayload(String s, Map<String, String> map, long l, String meterType) {
        JsonObject payload = new JsonObject();

        payload.addProperty("request_body", s);
        payload.addProperty("meter_type", meterType);
        payload.addProperty("ts", System.currentTimeMillis());
        payload.addProperty("value", l);
        payload.addProperty("topic_name", topicName);
        payload.addProperty("additional_params", new Gson().toJson(map));

        return payload.toString();
    }

    private void sendPostRequest(String jsonPayload) {
        try {
            URL url = new URL(serverAddress);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();

            connection.setRequestMethod("POST");
            connection.setDoOutput(true);

            try (OutputStream os = connection.getOutputStream();
                 OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8")) {
                osw.write(jsonPayload);
                osw.flush();
            }

            int responseCode = connection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                LOG.error("Failed to report metric. HTTP Response Code: " + responseCode);
            }

            connection.disconnect();

        } catch (Exception e) {
            LOG.error("Unable to produce to the HTTP endpoint" + e.getMessage());
        }
    }

    private String getServerUrlWithZK() {
        String serverUrl = "";
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

        try {
            List<String> servers = zkRegistry.discoverInstances();
            if (servers.size() == 0) {
                throw new RuntimeException("No servers available");
            }

            String serverDescriptor = zkRegistry.discoverInstances().get(0);
            serverUrl = "http://" + serverDescriptor.split(";")[0];

        } catch (Exception e) {
            LOG.error("Unable to get the server URL: " + e.getMessage());
        }
        return serverUrl;
    }

    @Override
    public void reportCounter(String s, Map<String, String> map, long l) {
        String payload = preparePayload(s, map, l, MetricType.COUNTER.name());
        sendPostRequest(payload);
    }

    @Override
    public void reportGauge(String s, Map<String, String> map, double v) {
    }

    @Override
    public void reportTimer(String s, Map<String, String> map, Duration duration) {

    }

    @Override
    public void reportHistogramValueSamples(String s, Map<String, String> map, Buckets buckets, double v, double v1, long l) {

    }

    @Override
    public void reportHistogramDurationSamples(String s, Map<String, String> map, Buckets buckets, Duration duration, Duration duration1, long l) {

    }

    @Override
    public Capabilities capabilities() {
        return null;
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }
}
