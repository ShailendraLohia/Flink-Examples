package TwitterApplication;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by shailendralohia on 7/18/18.
 */
public class TopHashTag {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        InputStream input = new FileInputStream("/Users/shailendralohia/Downloads/Twitter_creds.properties");

        Properties props=new Properties();

        props.load(input);

        props.setProperty(TwitterSource.CONSUMER_KEY, props.getProperty("CONSUMER_KEY"));
        props.setProperty(TwitterSource.CONSUMER_SECRET, props.getProperty("CONSUMER_SECRET"));
        props.setProperty(TwitterSource.TOKEN, props.getProperty("TOKEN"));
        props.setProperty(TwitterSource.TOKEN_SECRET, props.getProperty("TOKEN_SECRET"));

        env.addSource(new TwitterSource(props))
                .map(new MapToTweets())
                .flatMap(new FlatMapFunction<Tweet, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(Tweet tweet, Collector<Tuple2<String, Integer>> collector) {
                        for(String tag:tweet.getTags()) {
                            collector.collect(new Tuple2<>(tag,1));
                        }
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(1))
                .sum(1)
                .keyBy(1);
    }
}
