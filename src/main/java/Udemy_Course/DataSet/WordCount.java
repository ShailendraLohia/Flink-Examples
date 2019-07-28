package Udemy_Course.DataSet;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class WordCount {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment environment=ExecutionEnvironment.getExecutionEnvironment();
        //ParameterTool params=ParameterTool.fromArgs(args);
        DataSet<Tuple2<String, Integer>> text =environment.readTextFile("/Users/shailendralohia/Documents/Flink/src/main/java/Udemy_Course/InputFile/wc.txt")
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return s.startsWith("N");
                    }
                })
                .map(new Tokenizer())
                .groupBy(0)
                .sum(1);

        // The print() method triggers the execution by itself
        // so you don’t need to call it manually. Hence we can comment out execute method.
        text.print();
        //environment.execute("Word count example");


    }

}

