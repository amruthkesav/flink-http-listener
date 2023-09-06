package amrk7.exp.flink.functions.source.server;

import amrk7.exp.flink.functions.source.ZkRegistry;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.api.ACLProvider;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.ACL;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;

public class ExampleApp {

    public static class ConsoleSink implements SinkFunction<String> {
        @Override
        public void invoke(String value) {
            System.out.println(value); // Print the value to the console
        }
    }

    public static class SingleStringSource implements SourceFunction<String> {

        private volatile boolean isRunning = true;
        private final String value;

        public SingleStringSource(String value) {
            this.value = value;
        }

        @Override
        public void run(SourceFunction.SourceContext<String> ctx) throws Exception {
            if (isRunning) {
                ctx.collect(value);
                isRunning = false; // Stop after emitting the value
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> httpDataStream = env.addSource(new HttpSourceFunction(
                8090,
                new RequestTransformer(),
                new ZkRegistry(
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
                )
                )
        );
//        DataStream<String> httpDataStream = env.addSource(new SingleStringSource("hello"));
//        httpDataStream
//                .map((MapFunction<String, String>) s -> "Hello " + s)
//                        .addSink(new ConsoleSink());
        httpDataStream
                .addSink(new HttpMetricsSinkFunction());
        env.execute("hello");

// Process the httpDataStream as needed

    }
}
