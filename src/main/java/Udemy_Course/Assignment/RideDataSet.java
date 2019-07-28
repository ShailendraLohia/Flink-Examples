package Udemy_Course.Assignment;

import java.lang.Iterable;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;

public class RideDataSet {
    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple8<String, String, String, String, Boolean, String, String, Integer>> data = env.readTextFile("/Users/shailendralohia/Documents/Flink/src/main/java/Udemy_Course/InputFile/CabFlink.txt")
                .map(new MapFunction<String, Tuple8<String, String, String, String, Boolean, String, String, Integer>>()
                {
                    public Tuple8<String, String, String, String, Boolean, String, String, Integer> map(String value)
                    {
                        String[] words = value.split(",");
                        Boolean status = false;
                        if (words[4].equalsIgnoreCase("yes"))
                            status = true;
                        if (status)
                            return new Tuple8<String, String, String, String, Boolean, String, String, Integer>(words[0], words[1], words[2], words[3], status, words[5], words[6], Integer.parseInt(words[7]));
                        else
                            return new Tuple8<String, String, String, String, Boolean, String, String, Integer>(words[0], words[1], words[2], words[3], status, words[5], words[6], 0);
                    }
                });
//                .filter(new FilterFunction<Tuple8<String, String, String, String, Boolean, String, String, Integer>>()
//                {
//                    public boolean filter(Tuple8<String, String, String, String, Boolean, String, String, Integer> value)
//                    {
//                        return value.f4;
//                    }
//                });

        // most popular destination
        DataSet<Tuple8<String, String, String, String, Boolean, String, String, Integer>> popularDest =
                data.groupBy(6)
                        .sum(7)
                        .maxBy(7);

        popularDest.print();
    }
}
