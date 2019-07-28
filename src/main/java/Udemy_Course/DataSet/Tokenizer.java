package Udemy_Course.DataSet;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class Tokenizer implements MapFunction<String, Tuple2<String, Integer>> {
    public Tuple2<String, Integer> map(String value)
    {
        return new Tuple2(value, Integer.valueOf(1));
    }
}
