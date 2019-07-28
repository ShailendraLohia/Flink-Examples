package Udemy_Course.DataStream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ListStateExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment environment=
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Long>> stream=environment.socketTextStream("localhost",9090)
                .map(new MapFunction<String, Tuple2<Long,String>>() {
                    @Override
                    public Tuple2<Long,String> map(String value) throws Exception {
                        String[] words=value.split(",");
                        return new Tuple2<Long,String>(Long.parseLong(words[0]),words[1]);
                    }
                })
                .keyBy(0)
                .flatMap(new ListStatefulMap()); // converting stateless flatMap() to stateful using ListState

        stream.print();

        environment.execute();
    }
}

class ListStatefulMap extends RichFlatMapFunction<Tuple2<Long, String>, Tuple2<String, Long>> {
    private transient ValueState<Long> count;
    private transient ListState<Long> numbers;

    public void flatMap(Tuple2<Long, String> input, Collector<Tuple2<String, Long>> out)throws Exception
    {
        Long currValue = Long.parseLong(input.f1);
        Long currCount = count.value();

        currCount += 1;

        count.update(currCount);
        numbers.add(currValue);

        if (currCount >= 10)
        {
            Long sum = 0L;
            String numbersStr = "";
            for (Long number : numbers.get())
            {
                numbersStr = numbersStr + " " + number;
                sum = sum + number;
            }
            /* emit sum of last 10 elements */
            out.collect(new Tuple2<String, Long>(numbersStr, sum));
            /* clear value */
            count.clear();
            numbers.clear();
        }
    }
    public void open(Configuration conf)
    {
        ListStateDescriptor<Long> listDesc = new ListStateDescriptor<Long>("numbers", Long.class);
        numbers = getRuntimeContext().getListState(listDesc);

        ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<Long>( "count",  Long.class, 0L);
        count = getRuntimeContext().getState(descriptor2);
    }
}


