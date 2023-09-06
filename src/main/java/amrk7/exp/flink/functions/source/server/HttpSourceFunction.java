package amrk7.exp.flink.functions.source.server;

import amrk7.exp.flink.functions.source.ZkRegistry;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;

public class HttpSourceFunction implements SourceFunction<String> {
    private static final Logger LOG = LoggerFactory.getLogger(HttpSourceFunction.class);
    private final int port;
    private final RequestTransformer transformer;
    private Server server;

    public HttpSourceFunction(int port, RequestTransformer transformer, ZkRegistry registry) throws Exception {
        this.port = port;
        this.transformer = transformer;
        registry.addServerInstanceToAZkNamespace(getInstanceURI());
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        LOG.info("Starting a server at port: " + port);
        server = new Server(port);
        server.setHandler(new RequestHandler(transformer, ctx));

        try {
            server.start();
            server.join();
        } finally {
            server.stop();
        }
    }

    private String getInstanceURI() {
        return "localhost:" + port + ";";
    }

    @Override
    public void cancel() {
        if (server!= null) {
            try {
                server.stop();
            } catch (Exception e) {
                LOG.error("Exception encountered while closing server: " + e.getCause());
            }
        }
    }

    private static class RequestHandler extends AbstractHandler {
        private final SourceContext<String> context;
        private final RequestTransformer transformer;

        RequestHandler(RequestTransformer transformer, SourceContext<String> context) {
            this.context = context;
            this.transformer = transformer;
        }

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            BufferedReader reader = request.getReader();
            String line;
            while ((line = reader.readLine()) != null) {
                context.collect(transformer.apply(line));
            }
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);
        }
    }
}