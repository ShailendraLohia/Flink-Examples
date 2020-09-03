package Udemy_Course.DataStream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class KeyedProcessFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment=
                StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<String, String>> stream =
                //environment.socketTextStream("localhost",9090)
                        environment.fromElements("Hello Flink","Flink Dear", "Flink Learning ")
                        .map(new MapFunction<String, Tuple2<String, String>>() {
                            @Override
                            public Tuple2<String, String> map(String s) throws Exception {
                                String[] words= s.split(" ");

                                return new Tuple2<>(words[0],words[1]);
                            }
                        });

        DataStream<Tuple2<String, Long>> result = stream
                .keyBy(0)
                .process(new CountWithTimeoutFunction());

        result.print();

        environment.execute("Keyed Process Function Example");

    }
    public static class CountWithTimeoutFunction extends KeyedProcessFunction<Tuple, Tuple2<String, String>, Tuple2<String, Long>> {
        private ValueState<CountWithTimestamp> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
        }

        @Override
        public void processElement(
                Tuple2<String, String> value,
                Context ctx,
                Collector<Tuple2<String, Long>> out) throws Exception {

            // retrieve the current count
            CountWithTimestamp current = state.value();
            if (current == null) {
                current = new CountWithTimestamp();
                current.key = value.f0;
            }

            // update the state's count
            current.count++;

            // set the state's timestamp to the record's assigned event time timestamp
            long timerTimestamp = ctx.timerService().currentProcessingTime();
            current.lastModified = timerTimestamp;

            LocalDateTime date =
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(timerTimestamp), ZoneId.systemDefault());

            System.out.println("In Process element timerstamp: " + date);

            // write the state back
            state.update(current);

            // schedule the next timer 60 seconds from the current event time
            ctx.timerService().registerEventTimeTimer(current.lastModified + 1);
            //ctx.timerService().currentProcessingTime();

            //System.out.println("hello");
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<Tuple2<String, Long>> out) throws Exception {

            // get the state for the key that scheduled the timer
            CountWithTimestamp result = state.value();

            LocalDateTime date =
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());

            System.out.println("On timer method timestamp: " + date);

            // check if this is an outdated timer or the latest timer
            if (timestamp == result.lastModified + 1) {
                // emit the state on timeout
                System.out.println(result.key + "," + result.count);
                out.collect(new Tuple2<String, Long>(result.key, result.count));
            }
        }
    }
}

class CountWithTimestamp {

    public String key;
    public long count;
    public long lastModified;
}




