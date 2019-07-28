package Udemy_Course.Assignment;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//Average number of trips for each driver.  | average =  total no. of passengers drivers has picked / total no. of trips he made
public class AverageTripPerDriver {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment=
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String,Double>> stream=
                environment.readTextFile("/Users/shailendralohia/Documents/Flink/src/main/java/Udemy_Course/InputFile/CabFlink.txt")
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
                        })
                        .map(new MapFunction<Tuple8<String, String, String, String, Boolean, String, String, Integer>, Tuple3<String, Integer, Integer>>()
                        {
                            public Tuple3<String, Integer, Integer> map(Tuple8<String, String, String, String, Boolean, String, String, Integer> value)
                            {
                                // driver,trip_passengers,trip_count
                                return new Tuple3<String, Integer, Integer>(value.f3, value.f7, 1);
                            }
                        })
                        .keyBy(0)
                        .reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
                            @Override
                            public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> t0, Tuple3<String, Integer, Integer> t1) throws Exception {
                                return new Tuple3<>(t0.f0,t0.f1+t1.f1,t0.f2+t1.f2);
                            }
                        })
                        .map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Double>>()
                        {
                            public Tuple2<String, Double> map(Tuple3<String, Integer, Integer> value)
                            {
                                return new Tuple2<String, Double>(value.f0, ((value.f1*1.0)/value.f2));
                            }
                        });

        stream.print();
        environment.execute("Average Trip Per Driver");
    }
}
