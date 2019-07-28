package Udemy_Course.DataStream;
import java.sql.Timestamp;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TumblingWindowExample_EventTimeCharacteristic {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment=
                StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<Long,String>> sum=
                environment.socketTextStream("localhost",9090)
                .map(new MapFunction<String, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(String value) throws Exception {
                        String[] words= value.split(",");
                        return new Tuple2<Long,String>(Long.parseLong(words[0]),words[1]);
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Long, String>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<Long, String> t) {
                        return t.f0;
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))// Not a keyed stream. That's why windowAll() method used
                .reduce(new ReduceFunction<Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> reduce(Tuple2<Long, String> current, Tuple2<Long, String> preCalculated) throws Exception {
                        int num1 = Integer.parseInt(current.f1);
                        int num2 = Integer.parseInt(preCalculated.f1);
                        int sum = num1 + num2;
                        Timestamp t = new Timestamp(System.currentTimeMillis());
                        return new Tuple2<Long, String>(t.getTime(), "" + sum);
                    }
                });

        sum.print();
        environment.execute("windowing Operation");


    }
}
