package amrk7.exp.flink.functions.source.client;

import com.google.gson.Gson;
import com.uber.m3.tally.RootScopeBuilder;
import com.uber.m3.tally.Scope;
import com.uber.m3.util.Duration;

import java.util.HashMap;
import java.util.Map;

/**
 * HttpClientExample uses an HTTP metrics reporter to send metrics to a server.
 * It initializes a reporter, prepares metric data, and emits metrics in a loop with a specified reporting interval.
 */

public class HttpClientExample {

    private final static Integer REPORTING_INTERVAL_MILLIS = 1_000;
    public static void main(String[] args) throws Exception {

        HttpMetricsReporter httpMetricsReporter = new HttpMetricsReporter("hp-data-observability-XYZ");
        System.out.println("Reporter is ready...");

        Scope scope = new RootScopeBuilder().reporter(httpMetricsReporter).reportEvery(Duration.ofMillis(REPORTING_INTERVAL_MILLIS));

        Map<String, String> metrics = new HashMap<>();
        metrics.put("application_id", "abc_xyz");
        metrics.put("host_name", "phx2_abc");
        metrics.put("path", "my_custom_path");

        System.out.println("Payload is ready...");
        for(int i = 0; i< 100; i++) {
            scope.counter(new Gson().toJson(metrics)).inc(1);
            if (i%10 ==0) {
                Thread.sleep(2_000);
                // Sleeping for 2 seconds to flush the last 10 metrics since the reporting frequency is 1s.
                // We expect to see 10 metrics having 10 counts each reported to the server (Spaced 2 seconds apart).
                // At the end of this test, the server should have 10 messages with 100 value counts total.
            }
        }
    }
}