package Udemy_Course.DataStream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TumblingWindowExample_ProcessingTimeCharacteristic {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment=
                StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<Tuple5<String, String, String, Integer, Integer>> stream=
                environment.socketTextStream("localhost",9090)
                .map(new Splitter())
                .keyBy(0,1)
                //.timeWindow(Time.seconds(2))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .reduce(new ReduceLogic());

        stream.print();
        environment.execute("TimeProcessingExample");
    }
}
