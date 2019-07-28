package Udemy_Course.DataStream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ValueStateExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment environment=
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> stream=environment.socketTextStream("localhost",9090)
                .map(new MapFunction<String, Tuple2<Long,String>>() {
                    @Override
                    public Tuple2<Long,String> map(String value) throws Exception {
                        String[] words=value.split(",");
                        return new Tuple2<Long,String>(Long.parseLong(words[0]),words[1]);
                    }
                })
                .keyBy(0)
                .flatMap(new StatefulMap());

        stream.print();

        environment.execute();
    }
}

class StatefulMap extends RichFlatMapFunction<Tuple2<Long, String>, Long> {

    private transient ValueState<Long> sum;
    private transient ValueState<Long> count;

    @Override
    public void flatMap(Tuple2<Long, String> input, Collector<Long> out) throws Exception {
        Long currCount = count.value();        //   2
        Long currSum = sum.value();             //  4

        currCount += 1;
        currSum = currSum + Long.parseLong(input.f1);

        count.update(currCount);
        sum.update(currSum);

        if (currCount >= 10) { // Sum every 10 records per key
            /* emit sum of last 10 elements */
            out.collect(sum.value());
            /* clear value */
            count.clear();
            sum.clear();
        }
    }
        public void open(Configuration conf)
        {
            ValueStateDescriptor<Long> descriptor =new ValueStateDescriptor<Long>("sum", TypeInformation.of(new TypeHint<Long>() {}), 0L);
            sum = getRuntimeContext().getState(descriptor);

            ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<Long>( "count",  TypeInformation.of(new TypeHint<Long>() {}), 0L);
            count = getRuntimeContext().getState(descriptor2);
        }

}
