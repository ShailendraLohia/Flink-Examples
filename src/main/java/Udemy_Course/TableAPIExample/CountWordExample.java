package Udemy_Course.TableAPIExample;


import org.apache.calcite.avatica.proto.Common;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


public class CountWordExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment=StreamTableEnvironment.create(environment);


        DataStream<WC> streamOfWords =
                environment.fromElements(
                        new WC("Hello",1L),
                        new WC("Howdy",1L),
                        new WC("Hello",1L),
                        new WC("Hello",1L));





        //DataStream<String> stream= environment.fromElements("Hello","Hello","Howdy","Hello");
        Table t1 = streamTableEnvironment.fromDataStream(streamOfWords);
        //Table result = streamTableEnvironment.sqlQuery("select word, count(word) as wordcount from " + t1 + " group by word");
        Table result = t1.groupBy("word")
                .select("word.count as count, word");
               // .filter("count < 2");

        //streamTableEnvironment.registerDataStream("Count", streamOfWords);

        //Table t1 = streamTableEnvironment.scan("Count");

        //Table result= t1.select("count(word)");
        //Table result= streamTableEnvironment.sqlQuery("SELECT * FROM Count WHERE count<2");

//        Table table= streamTableEnvironment.fromDataStream(stream);
//
//        Table countWords = table.select("count(f0)");

        streamTableEnvironment.toRetractStream(result, Row.class ).print();

        //streamTableEnvironment.toAppendStream(result, Row.class ).print();

        environment.execute();

    }



    public static class WC {

        public String word;
        public Long count;

        public WC() {}

        public WC(String word,Long count) {
            this.word=word;
            this.count=count;
        }
    }
}

