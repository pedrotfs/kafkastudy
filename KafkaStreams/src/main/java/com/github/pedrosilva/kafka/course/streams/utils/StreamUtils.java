package com.github.pedrosilva.kafka.course.streams.utils;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamUtils {

    private static final String BOOTSTRAP_DEFAULT = "127.0.0.1:9092";

    private static final Logger LOG = LoggerFactory.getLogger(StreamUtils.class);

    public static Properties getKafkaStreamProperties(final String applicationId)
    {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_DEFAULT);
        return properties;
    }

    public static Properties getKafkaStreamProperties(final String bootstrapServer, final String applicationId)
    {
        Properties properties = getKafkaStreamProperties(applicationId);
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        return properties;
    }

    public static int extractFollowerCountFromTweet(final String tweet)
    {
        JsonParser parser = new JsonParser();
        int result = 0;
        try {
            result = parser.parse(tweet).getAsJsonObject().get("user").getAsJsonObject().get("followers_count").getAsInt();
        } catch (NullPointerException e) {
            LOG.warn("no twitter followers or no user detected");
        } catch (IllegalStateException e) {
            LOG.warn("Junk found. skipping");
        }
        return result;
    }

}
