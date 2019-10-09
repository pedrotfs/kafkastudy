package com.github.pedrosilva.kafka.course.streams.start;

import com.github.pedrosilva.kafka.course.streams.utils.StreamUtils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Collections;

public class StartKafkaStreams {

    private static final int FOLLOWERS = 10000;

    private static final String APPLICATION_ID = "tweet.filter.followers";

    private static final String TOPIC_ORIGIN = "tweets"; //please create the topic first. 6 partitions, replication factor 3

    private static final String TOPIC_DESTINY = "tweets.popular"; //please create the topic first. 3 partitions, replication factor 1

    private StartKafkaStreams() {
        //ok
    }

    public static void main(String[] args) {
        new StartKafkaStreams().run();
    }

    private void run()
    {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> inputTopic = streamsBuilder.stream(Collections.singletonList(TOPIC_ORIGIN));
        KStream<String, String> filteredStream = inputTopic.filter(
                (k, tweet) -> StreamUtils.extractFollowerCountFromTweet(tweet) > FOLLOWERS
        );
        filteredStream.to(TOPIC_DESTINY);
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), StreamUtils.getKafkaStreamProperties(APPLICATION_ID));
        kafkaStreams.start();
    }
}
