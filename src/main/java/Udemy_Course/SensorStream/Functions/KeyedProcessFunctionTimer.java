package Udemy_Course.SensorStream.Functions;

import Udemy_Course.SensorStream.util.SensorReading;
import Udemy_Course.SensorStream.util.SensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KeyedProcessFunctionTimer {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // use event time for the application
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<SensorReading> sensorData=
                env.addSource(new SensorSource());

        DataStream<java.lang.String> warnings = sensorData
                .keyBy(r -> r.id)
                .process(new TempIncreaseAlertFunction());

        warnings.print();
        env.execute("Keyed Process Function execution");
    }
    public static class TempIncreaseAlertFunction extends KeyedProcessFunction<String, SensorReading, String> {

        private ValueState<Double> lastTemp;
        private ValueState<Long> currentTimer;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp", Types.DOUBLE));
            currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", org.apache.flink.api.common.typeinfo.Types.LONG));
        }

        @Override
        public void processElement(
                SensorReading r,
                Context ctx,
                Collector<String> out) throws Exception {

            // get previous Temp
            Double prevTemp = lastTemp.value();

            if(prevTemp==null)
            {
                prevTemp=0.0;
            }
            // update last temp
            lastTemp.update(r.temperature);

            Long curTimerTimestamp = currentTimer.value();

            if(curTimerTimestamp == null) {
                curTimerTimestamp = 0L;
            }

            if(prevTemp==0.0 ) {

            }
            else if(r.temperature < prevTemp) {
                ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp);
                currentTimer.clear();
            }
            else if(r.temperature > prevTemp && curTimerTimestamp == 0) {
                Long timerTs = ctx.timerService().currentProcessingTime() + 1000;
                //System.out.println("Sensor Id: " + r.id + " Register Time: " + timerTs);
                ctx.timerService().registerProcessingTimeTimer(timerTs);
                currentTimer.update(timerTs);

            }
        }

        @Override
        public void onTimer(
                long ts,
                OnTimerContext ctx,
                Collector<java.lang.String> out) throws Exception {

            //System.out.println("Time when Timer Fire: " + ts);
            out.collect("Temperature of sensor ' " + ctx.getCurrentKey() + " ' monotonically increased for 1 second.");
            currentTimer.clear();

        }
    }
}
