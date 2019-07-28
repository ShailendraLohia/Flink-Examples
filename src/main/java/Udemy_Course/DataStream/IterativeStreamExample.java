package Udemy_Course.DataStream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class IterativeStreamExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment environment=
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Generate the sequence of stream data
        DataStream<Tuple2<Long,Integer>> stream=
                environment.generateSequence(0,5)
                .map(new MapFunction<Long, Tuple2<Long,Integer>>() {
                    public Tuple2<Long,Integer> map(Long value) {
                        return new Tuple2<>(value,0);
                    } // Here value is generated by generateSequence method and 0 is initial counter (number of iteration required to make 'value' 10)
                });

        //prepare stream for iteration
        IterativeStream<Tuple2<Long,Integer>> iterativeStream =
                stream.iterate(5000);

        // Define Iteration
        DataStream<Tuple2<Long, Integer>> plusOne =
                iterativeStream.map(new MapFunction<Tuple2<Long,Integer>, Tuple2<Long, Integer>>()
        {
            public Tuple2<Long, Integer> map(Tuple2<Long, Integer> value)
            {
                if (value.f0 == 10)
                    return value;
                else
                    return new Tuple2<Long, Integer>(value.f0+1, value.f1+1); // value will be incremented by one.
            }
        });   //   plusone    1,1   2,1  3,1   4,1   5,1   6,1

        // stream data that needs to be used in next iteration (
        DataStream<Tuple2<Long, Integer>> notEqualtoten =
                plusOne.filter(new FilterFunction<Tuple2<Long, Integer>>()
        {
            public boolean filter(Tuple2<Long, Integer> value)
            {
                if (value.f0 == 10)
                    return false;
                else
                    return true;
            }
        });
        // feed data back to next iteration. closeWith() function send data back to iterative stream.
        iterativeStream.closeWith(notEqualtoten);

        // data not feedback to iteration
        DataStream<Tuple2<Long, Integer>> equaltoten =
                plusOne.filter(new FilterFunction<Tuple2<Long, Integer>>()
        {
            public boolean filter(Tuple2<Long, Integer> value)
            {
                if (value.f0 == 10)
                    return true;
                else return false;
            }});

        equaltoten.print();
        environment.execute("Iteration Operator");
    }
}