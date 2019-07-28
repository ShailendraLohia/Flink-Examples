package TwitterApplication;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by shailendralohia on 7/9/18.
 */
public class FilterEnglishTweets {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        InputStream input = new FileInputStream("/Users/shailendralohia/Downloads/Twitter_creds.properties");

        Properties props=new Properties();

        props.load(input);

        props.setProperty(TwitterSource.CONSUMER_KEY, props.getProperty("CONSUMER_KEY"));
        props.setProperty(TwitterSource.CONSUMER_SECRET, props.getProperty("CONSUMER_SECRET"));
        props.setProperty(TwitterSource.TOKEN, props.getProperty("TOKEN"));
        props.setProperty(TwitterSource.TOKEN_SECRET, props.getProperty("TOKEN_SECRET"));

        //TwitterSource twitterSource=new TwitterSource(props);
        env.addSource(new TwitterSource(props))
                .map(new MapToTweets())
                .filter(new FilterFunction<Tweet>() {
                    @Override
                    public boolean filter(Tweet tweet) throws Exception {
                        return tweet.getLanguage().equals("en");
                    }
                })
                .print();

        env.execute();
    }
}
