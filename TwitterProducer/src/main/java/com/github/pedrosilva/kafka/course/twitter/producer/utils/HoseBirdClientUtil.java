package com.github.pedrosilva.kafka.course.twitter.producer.utils;

import com.github.pedrosilva.kafka.course.util.PropertyLoader;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

public class HoseBirdClientUtil {

    public static Client createHBCClient(BlockingQueue<String> messageQueue, List<String> searchTerms) throws IOException {
        // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        //        List<Long> followings = Lists.newArrayList(1234L, 566788L); //track followings
        //        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(searchTerms);

        Properties properties = PropertyLoader.getConfigurations();

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(properties.getProperty("consumer.key"), properties.getProperty("consumer.secret"), properties.getProperty("token"), properties.getProperty("token.secret"));

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(messageQueue));
        //.eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        // Attempts to establish a connection.
        return builder.build();
    }

}
