package Udemy_Course.DataStream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class SplitOperator {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment environment=
                StreamExecutionEnvironment.getExecutionEnvironment();

        // To split stream data, use SplitStream API.
        SplitStream<Integer> splitStream= environment.readTextFile("/Users/shailendralohia/Documents/Flink/src/main/java/Udemy_Course/InputFile/OddEvenInputData.txt")
                .map(new MapFunction<String, Integer>() {
                    public Integer map(String value) {
                        return Integer.parseInt(value);
                    }
                })
                .split(new OutputSelector<Integer>() { // Split operator implements OutputSelector interface
                    @Override
                    public Iterable<String> select(Integer value) { // OutputSelector Interface has select method.
                        List<String> out = new ArrayList<String>();
                        if (value%2 == 0)
                            out.add("even");              // can add label element like "even" or "odd"  --> even 454   odd 565 etc
                        else
                            out.add("odd");
                        return out;
                    }
                });

        DataStream<Integer> evenStream=splitStream.select("even");
        DataStream<Integer> oddStream=splitStream.select("odd");

        evenStream.print();
        oddStream.print();

        environment.execute("Split operator example");

    }
}
