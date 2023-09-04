package amrk7.exp.flink.functions.source;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import com.uber.m3.tally.Buckets;
import com.uber.m3.tally.Capabilities;
import com.uber.m3.tally.StatsReporter;
import com.uber.m3.util.Duration;
import com.google.gson.Gson;


public class HttpMetricsReporter implements StatsReporter, AutoCloseable{

    private final String topicName;
    private final String serverAddress;

    public HttpMetricsReporter(String topicName) {
        this.topicName = topicName;
        serverAddress = "";
    }

    private void preparePayload(String s, Map<String, String> map, long l, String meterType) {

    }

    private void sendPostRequest(String jsonPayload) throws Exception {
        URL url = new URL(serverAddress);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Accept", "application/json");
        connection.setDoOutput(true);

        try (OutputStream os = connection.getOutputStream();
             OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8")) {
            osw.write(jsonPayload);
            osw.flush();
        }

        int responseCode = connection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            LOG.info("Metric reported successfully.");
        } else {
            System.err.println("Failed to report metric. HTTP Response Code: " + responseCode);
        }

        connection.disconnect();
    }


    @Override
    public void reportCounter(String s, Map<String, String> map, long l) {
        String payload = preparePayload(s, map, l, "COUNTER");
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
