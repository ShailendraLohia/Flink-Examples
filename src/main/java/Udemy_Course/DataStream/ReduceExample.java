package Udemy_Course.DataStream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment=StreamExecutionEnvironment.
                getExecutionEnvironment();

        DataStream<Tuple5<String, String, String, Integer, Integer>> stream =
                environment.readTextFile("/Users/shailendralohia/Documents/Flink/src/main/java/Udemy_Course/InputFile/AverageProfitData.txt")
                .map(new MapFunction<String, Tuple5<String, String, String, Integer, Integer>>() {

                    public Tuple5<String, String, String, Integer, Integer> map(String value) {

                        String[] words=value.split(",");
                        return new Tuple5<String, String, String, Integer, Integer>
                                (words[1],words[2],words[3],Integer.parseInt(words[4]),1);
                    }
                })
                .keyBy(0)
                .reduce(new ReduceLogic()); // group by month

        //Average Profit
        DataStream<Tuple2<String,Double>> avgProfitPerMonth=
                stream.map(new MapFunction<Tuple5<String, String, String, Integer, Integer>,
                        Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple5<String, String, String, Integer, Integer> inputData) throws Exception {
                        return new Tuple2<>(inputData.f0,new Double((inputData.f3*1.0)/inputData.f4));
                    }
                });


        avgProfitPerMonth.print(); //trigger the code

        environment.execute("Executing Stream example");


    }


}

class ReduceLogic implements ReduceFunction<Tuple5<String, String, String, Integer, Integer>> {
    public Tuple5<String, String, String, Integer, Integer> reduce(
                    Tuple5<String, String, String, Integer, Integer> current, // Current record/data
                    Tuple5<String, String, String, Integer, Integer> pre_result) // Pre Calculated data. Current data get added here if key found.
    {
        return new Tuple5<String, String, String, Integer, Integer>(
                current.f0,current.f1, current.f2, current.f3 + pre_result.f3, current.f4 + pre_result.f4);
    }
}



