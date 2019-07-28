package Udemy_Course.DataStream;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.common.functions.MapFunction;

public class Splitter implements MapFunction<String,
        Tuple5<String, String, String, Integer, Integer>> {

    public Tuple5<String, String, String, Integer, Integer> map(String value) throws Exception{

        String[] words = value.split(",");
        return new Tuple5<String, String, String, Integer, Integer>
                (words[1],words[2],words[3],Integer.parseInt(words[4]),1);
    }
}
