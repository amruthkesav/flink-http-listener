package amrk7.exp.flink.functions.source.server;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import hpw_shaded.com.uber.data.heatpipe.configuration.Heatpipe4JConfig;
import hpw_shaded.com.uber.data.heatpipe.configuration.PropertiesHeatpipeConfiguration;
import hpw_shaded.com.uber.data.heatpipe.producer.HeatpipeProducer;
import hpw_shaded.com.uber.data.heatpipe.producer.HeatpipeProducerFactory;
import com.uber.m3.tally.Buckets;
import com.uber.m3.tally.Capabilities;
import com.uber.m3.tally.StatsReporter;
import com.uber.m3.util.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaMetricsReporter implements StatsReporter, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsReporter.class);

  private HeatpipeProducer producer;

  KafkaMetricsReporter(String topicName) {
    Properties properties = new Properties();
    properties.setProperty("heatpipe.app_id", "data-observability");
    properties.setProperty("kafka.disableShutdownHook", "true");
    Heatpipe4JConfig config = new PropertiesHeatpipeConfiguration(properties);
    try {
      // Todo: Accept the properties and topic version from the client, for now hardcoding.
      HeatpipeProducerFactory factory = new HeatpipeProducerFactory(config);
      this.producer = factory.get(topicName, 1);
    } catch (Exception e) {
      LOG.info(String.valueOf(e));
    }
  }

  @Override
  public void reportCounter(String s, Map<String, String> map, long l) {
    try {
      System.out.println("Got request to report counter, " + s + " " + l + " " + map);
      // Todo: use the tagged props for more customizations
      pushToKafka(s, l);
    } catch (Exception e) {
      LOG.error("Exception was: " + e.getMessage() + e.getCause() + Arrays.toString(e.getStackTrace()));
    }

  }

  @Override
  public void reportGauge(String s, Map<String, String> map, double v) {
  }

  @Override
  public void reportTimer(String s, Map<String, String> map, Duration duration) {}

  @Override
  public void reportHistogramValueSamples(
      String s, Map<String, String> map, Buckets buckets, double v, double v1, long l) {}

  @Override
  public void reportHistogramDurationSamples(
      String s,
      Map<String, String> map,
      Buckets buckets,
      Duration duration,
      Duration duration1,
      long l) {}

  @Override
  public Capabilities capabilities() {
    return null;
  }

  @Override
  public void flush() {}

  @Override
  public void close() {}

  private void pushToKafka(String tag, long value) throws Exception {
    try {
      if (this.producer == null) {
        LOG.info("Unable to create kafka producer");
      }
      Gson gson = new Gson();
      Map<String, Object> payload = gson.fromJson(tag, new TypeToken<Map<String, Object>>(){}.getType());
      // Todo: Somehow regulate this from client side
      payload.put("value", value);
      this.producer.produce(payload);
    } catch (Exception e) {
        LOG.info(String.valueOf(e.getCause()));
    }
  }
}
