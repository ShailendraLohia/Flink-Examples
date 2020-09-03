package Udemy_Course.TableAPIExample;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class CountWord_DataSet_Example {
    public static void main(String[] args) throws Exception{

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(environment);
                //TableEnvironment.getTableEnvironment(environment);

        DataSet<WC> input = environment.fromElements(
                new WC("Hello", 1L),
                new WC("Ciao", 1L),
                new WC("Hello", 1L));

        Table table = batchTableEnvironment.fromDataSet(input,"word, count");

        Table filtered = table
                .groupBy("word")
                .select("word.count as count, word")
                .filter("count = 2");

        DataSet<WC> result = batchTableEnvironment.toDataSet(filtered, WC.class);
        result.print();
    }

    public static class WC {

        private String word;
        private Long count;

        public WC() {}

        public WC(String word,Long count) {
            this.word=word;
            this.count=count;
        }
    }
}
