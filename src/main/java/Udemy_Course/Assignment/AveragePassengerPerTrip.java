package Udemy_Course.Assignment;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AveragePassengerPerTrip {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment environment=
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String,Integer,Integer>> stream=
                environment.readTextFile("/Users/shailendralohia/Documents/Flink/src/main/java/Udemy_Course/InputFile/CabFlink.txt")
                .map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> map(String s) throws Exception {

                        String[] words= s.split(",");

                        return new Tuple3<>(words[5],Integer.parseInt(words[7]),1);
                    }
                })
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> t0, Tuple3<String, Integer, Integer> t1) throws Exception {

                        return new Tuple3<>(t0.f0,t0.f1+t1.f1,t0.f2+t1.f2);
                    }
                });

        DataStream<Tuple2<String,Double>> average=
                stream.map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple3<String, Integer, Integer> value) throws Exception {
                        return new Tuple2<String, Double>(value.f0, ((value.f1*1.0)/value.f2));
                    }
                });

        average.print();
        environment.execute("AveragePassengerPerTrip");
    }
}
