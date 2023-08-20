package amrk7.exp.flink.functions.source;

import java.io.Serializable;
import java.util.function.Function;

public class RequestTransformer implements Serializable, Function<String, String> {
    @Override
    public String apply(String s) {
        return s;
    }
}
