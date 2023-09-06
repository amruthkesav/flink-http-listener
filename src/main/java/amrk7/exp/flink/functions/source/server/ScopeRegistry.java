package amrk7.exp.flink.functions.source.server;

import com.uber.m3.tally.RootScopeBuilder;
import com.uber.m3.tally.Scope;
import com.uber.m3.util.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ScopeRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(ScopeRegistry.class);

    private static final ScopeRegistry instance = new ScopeRegistry();
    private static final Map<String, Scope> registry = new ConcurrentHashMap<>();

    private ScopeRegistry() {}

    public static Scope getOrCreateReporter(String topicName) {
        return registry.computeIfAbsent(topicName, key -> createScope(topicName));
    }

    private static Scope createScope(String topicName) {
        System.out.println("Calling create reporter");
        Scope scope = null;
        try {
            // Todo: Supply client provided properties along with topic name
            scope = new RootScopeBuilder().reporter(new KafkaMetricsReporter(topicName))
                    // make reporting interval client configurable
                    .reportEvery(Duration.ofMillis(10_000))
                    .subScope(topicName);

        } catch (Exception e) {
            LOG.error("Failed to create Kafka reporter for topic: " + topicName, e);
        }
        return scope;
    }
}
