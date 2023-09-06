package amrk7.exp.flink.functions.source.server;

import amrk7.exp.flink.functions.source.MetricType;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.uber.m3.tally.RootScopeBuilder;
import com.uber.m3.tally.Scope;
import com.uber.m3.util.Duration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Map;

public class HttpMetricsSinkFunction implements SinkFunction<String> {

    @Override
    public void invoke(String value, SinkFunction.Context context) {
        Gson gson = new Gson();
        JsonObject payload = gson.fromJson(value, JsonObject.class);

        String requestBody = payload.get("request_body").getAsString();
        String meterType = payload.get("meter_type").getAsString();
        long ts = payload.get("ts").getAsLong();
        long count = payload.get("value").getAsLong();
        String topicName = payload.get("topic_name").getAsString();
        String additionalParamsJson = payload.get("additional_params").getAsString();

        Map<String, String> additionalParams = gson.fromJson(additionalParamsJson, new TypeToken<Map<String, String>>() {
        }.getType());

        System.out.println("request_body: " + requestBody);
        System.out.println("meter_type: " + meterType);
        System.out.println("ts: " + ts);
        System.out.println("value: " + count);
        System.out.println("topic_name: " + topicName);

        Scope scope = ScopeRegistry.getOrCreateReporter(topicName);

        System.out.println("Printing scope hash " + scope.hashCode());
        switch (MetricType.valueOf(meterType)) {
            case COUNTER:
                scope.counter(requestBody).inc(count);
                break;
            case GAUGE:
                scope.gauge(requestBody).update(count);
                break;
            case TIMER:
                // Enforce millis from client side
                scope.timer(requestBody).record(Duration.ofMillis(count));
                break;
            case HISTOGRAM:
                // Todo: Propagate buckets from client for histogram
                scope.histogram(requestBody, null).recordValue(count);
                break;
            default:
                break;
        }


    }

}
