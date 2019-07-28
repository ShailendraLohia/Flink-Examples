package Udemy_Course.DataStream;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;


public class WatermarkExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.getConfig().setAutoWatermarkInterval(10000L);
       // environment.setMaxParallelism(1);

        DataStream<Tuple2<Long,String>> sum =
                environment.socketTextStream("localhost",9090)
                .map(new MapFunction<String, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(String s) throws Exception {
                        String[] words = s.split(",");

                        return new Tuple2<>(Long.parseLong(words[0]),words[1]);
                    }
                })
                .assignTimestampsAndWatermarks(new DemoWatermark())
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> reduce(Tuple2<Long, String> currentRecord, Tuple2<Long, String> calculatedRecord) throws Exception {
                        int num1 = Integer.parseInt(currentRecord.f1);
                        int num2 = Integer.parseInt(calculatedRecord.f1);
                        int sum = num1 + num2;
                        Timestamp t = new Timestamp(System.currentTimeMillis());
                        return new Tuple2<Long, String>(t.getTime(), "" + sum);

                    }
                });

        sum.print();
        environment.execute("watermark example");
    }
}

class DemoWatermark implements AssignerWithPeriodicWatermarks<Tuple2<Long,String>> {
    private final long allowedlatetime = 3500; // 3.5 seconds

    private long currentMaxTimestamp=0;

    public long extractTimestamp(Tuple2<Long, String> element, long previousElementTimestamp)
    {
        long timestamp = element.f0;
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }

    public Watermark getCurrentWatermark()
    {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        return new Watermark(currentMaxTimestamp - allowedlatetime);
    }
}

