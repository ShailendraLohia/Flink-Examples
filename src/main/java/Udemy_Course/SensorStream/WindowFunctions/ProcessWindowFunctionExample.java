package Udemy_Course.SensorStream.WindowFunctions;

import Udemy_Course.SensorStream.util.SensorReading;
import Udemy_Course.SensorStream.util.SensorSource;
import Udemy_Course.SensorStream.util.SensorTimeAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Collections;

public class ProcessWindowFunctionExample {
    public static void main(String[] args) throws Exception{
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // use event time for the application
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // configure watermark interval
        env.getConfig().setAutoWatermarkInterval(1000L);

        // checkpoint every 10 seconds
        env.getCheckpointConfig().setCheckpointInterval(10 * 1000);

        // ingest sensor stream
        DataStream<SensorReading> sensorData =
                env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());


        DataStream<MinMaxTemp> minMaxTempPerWindow = sensorData
                .keyBy(r ->r.id)
                .timeWindow(Time.seconds(5))
                .process(new HighAndLowTempProcessFunction());
    }

    public static class HighAndLowTempProcessFunction extends
            ProcessWindowFunction <SensorReading, MinMaxTemp, String, TimeWindow> {

        @Override
        public void process(String key, Context ctx,
                            Iterable<SensorReading> vals, Collector<MinMaxTemp> out) {


            //TODO: This logic is not working yet
//            long windowEnd= ctx.window().getEnd();
//
//            double min=0.0;
//            double max=1.0;
            //out.collect(key, min, max, windowEnd);
        }

    }
}

class MinMaxTemp {
    String id;
    double min;
    double max;
    long endTs;
}
