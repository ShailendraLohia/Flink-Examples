package Udemy_Course.DataStream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Properties;

public class TumblingCountingWindowApp {

    public static void main(String[] args) throws Exception{

        Properties properties = new Properties();

        properties.put("bootstrap.servers","localhost:9092");
        properties.put("security.protocol","PLAINTEXT");
        //properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        String kafkaInputTopic = "pos.data.json";


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.getConfig().setAutoWatermarkInterval(10000L);

        DataStream<String> stream = env.addSource(
                new FlinkKafkaConsumer011<String>(kafkaInputTopic,new SimpleStringSchema(),properties)
        );

        DataStream<Tuple3<String,Long,Long>> mapToInvoiceObject= stream.map(new InvoiceMap())
                .map(new mapFunction())
                .assignTimestampsAndWatermarks(new InvoiceTimeExtractor())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .reduce(new CountWindowUsingReduce());

        mapToInvoiceObject.print();

        env.execute("Learning window concept using Event time ");

    }
}

class InvoiceMap implements MapFunction<String,SimpleInvoice> {

    static private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public SimpleInvoice map(String data) throws Exception{

        JsonNode node = mapper.readTree(data);

        JsonNode createdTime = node.get("CreatedTime");
        JsonNode invoiceNumber = node.get("InvoiceNumber");
        JsonNode storeID = node.get("StoreID");
        JsonNode totalAmount = node.get("TotalAmount");

        SimpleInvoice simpleInvoice = new SimpleInvoice();


        simpleInvoice.setCreatedTime(Long.parseLong(createdTime.textValue()));
        simpleInvoice.setInvoiceNumber(invoiceNumber.toString());
        simpleInvoice.setStoreID(storeID.textValue());
        simpleInvoice.setTotalAmount(totalAmount.doubleValue());


        return simpleInvoice;
        }

}

class mapFunction implements MapFunction<SimpleInvoice, Tuple3<String,Long,Long>> {
    @Override
    public Tuple3<String,Long,Long> map(SimpleInvoice s) throws Exception {
        return new Tuple3<String,Long,Long>(s.getStoreID(),1L,s.getCreatedTime());
    }
}

class CountWindowUsingReduce implements ReduceFunction<Tuple3<String,Long,Long>> {

    @Override
    public Tuple3<String,Long,Long> reduce(Tuple3<String,Long,Long> result,Tuple3<String,Long,Long> prevData) {

        Long updatedCount= prevData.f1 + result.f1;
        return new Tuple3<String,Long,Long>(result.f0,updatedCount,result.f2);
    }
}

class InvoiceTimeExtractor implements AssignerWithPeriodicWatermarks<Tuple3<String,Long,Long>> {
    private long currentMaxTimestamp=0;

    @Override
    public long extractTimestamp(Tuple3<String,Long,Long> element, long previousElementTimestamp)
    {
        long timestamp = element.f2;

        LocalDateTime date =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        System.out.println("StoreID: " + element.f0 + "," + "createdTime: " + date);

        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);

        LocalDateTime currentMaxTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(currentMaxTimestamp), ZoneId.systemDefault());
        System.out.println("StoreID: " + element.f0 + "," + "currentMaxTimestamp: " + currentMaxTime);

        return timestamp;
    }

    @Override
    public Watermark getCurrentWatermark()
    {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        return new Watermark(currentMaxTimestamp);
    }
}

//Data Set to test
/*

{"InvoiceNumber": 101,"CreatedTime": "1549360860000","StoreID": "STR1534","TotalAmount": 1920}
{"InvoiceNumber": 102,"CreatedTime": "1549360900000","StoreID": "STR1535","TotalAmount": 1860}
{"InvoiceNumber": 103,"CreatedTime": "1549360999000","StoreID": "STR1534","TotalAmount": 2400}

{"InvoiceNumber": 106,"CreatedTime": "1549361600000","StoreID": "STR1536","TotalAmount": 9365}

{"InvoiceNumber": 106,"CreatedTime": "1549361920000","StoreID": "STR1534","TotalAmount": 9365}
{"InvoiceNumber": 104,"CreatedTime": "1549361160000","StoreID": "STR1536","TotalAmount": 8936}
{"InvoiceNumber": 105,"CreatedTime": "1549361270000","StoreID": "STR1534","TotalAmount": 6375}
{"InvoiceNumber": 106,"CreatedTime": "1549361370000","StoreID": "STR1536","TotalAmount": 9365}

https://github.com/LearningJournal/Kafka-Streams-Real-time-Stream-Processing/blob/master/counting-window/src/main/resources/data/sample-invoices.txt
 */
