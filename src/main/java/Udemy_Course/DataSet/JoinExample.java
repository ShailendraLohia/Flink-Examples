package Udemy_Course.DataSet;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class JoinExample {
    public static void main(String[] args) throws Exception{

        ExecutionEnvironment environment=ExecutionEnvironment.getExecutionEnvironment();

        // Read Person Data
        DataSet<Tuple2<Integer,String>> personData=
                environment.readTextFile("/Users/shailendralohia/Documents/Flink/src/main/java/Udemy_Course/InputFile/Person.txt")
                       .map(new MapFunction<String, Tuple2<Integer,String>>() {
                           public Tuple2<Integer,String> map(String value) {
                               String[] words=value.split(",");
                               return new Tuple2<Integer,String>(Integer.parseInt(words[0]),words[1]);
                           }
                       });

        //Read Location Data
        DataSet<Tuple2<String, Integer>> locationData=
                environment.readTextFile("/Users/shailendralohia/Documents/Flink/src/main/java/Udemy_Course/InputFile/Location.txt")
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] words= value.split(",");
                         return new Tuple2<String, Integer>(words[0], Integer.parseInt(words[1])) ;
                    }
                });

        // Join two data
        DataSet<Tuple3<Integer,String,String>> joinedData=
                personData.join(locationData)
                .where(0)
                .equalTo(1)
//                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<String, Integer>, Tuple3<Integer, String, String>>() {
//                    @Override
//                    public Tuple3<Integer, String, String> join(
//                            Tuple2<Integer, String> person,
//                            Tuple2<String, Integer> location) throws Exception {
//
//                        return new Tuple3<Integer, String, String>(person.f0,person.f1,location.f0);
//                    }
//                });
                .with(new JoinTwoFields());

        joinedData.print();
    }
}

class JoinTwoFields implements JoinFunction<Tuple2<Integer, String>, Tuple2<String, Integer>, Tuple3<Integer, String, String>> {
    public Tuple3<Integer, String, String> join(
            Tuple2<Integer, String> person,
            Tuple2<String, Integer> location) throws Exception {

        return new Tuple3<Integer, String, String>(person.f0,person.f1,location.f0);
    }

    // Note: Learn about Joint Hints API which Flink provides

}
