package TwitterApplication;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

/**
 * Created by shailendralohia on 7/17/18.
 */
public class NumberOfTweetsPerLanguage {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        InputStream input = new FileInputStream("/Users/shailendralohia/Downloads/Twitter_creds.properties");

        Properties props=new Properties();

        props.load(input);

        props.setProperty(TwitterSource.CONSUMER_KEY, props.getProperty("CONSUMER_KEY"));
        props.setProperty(TwitterSource.CONSUMER_SECRET, props.getProperty("CONSUMER_SECRET"));
        props.setProperty(TwitterSource.TOKEN, props.getProperty("TOKEN"));
        props.setProperty(TwitterSource.TOKEN_SECRET, props.getProperty("TOKEN_SECRET"));

        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        DataStream<String> stream=env.addSource(new TwitterSource(props));

        //env.addSource(new TwitterSource(props))
        stream.map(new MapToTweets())
                .keyBy(new KeySelector<Tweet, String>() {
                    @Override
                    public String getKey(Tweet tweet) throws Exception {
                        return tweet.getLanguage();
                    }
                })
                .timeWindow(Time.seconds(1))
                .apply(new WindowFunction<Tweet, Tuple3<String, Long, Date>, String, TimeWindow>() {
                    @Override
                    public void apply(String language, TimeWindow window, Iterable<Tweet> input, Collector<Tuple3<String, Long, Date>> out) throws Exception {
                        long count=0;
                        for(Tweet tweet : input) {
                            System.out.println(count++  + "Language: " +language);
                        }
                        out.collect(new Tuple3<>(language, count, new Date(window.getEnd())));
                    }
                })
                .print();
        env.execute();
    }
}
