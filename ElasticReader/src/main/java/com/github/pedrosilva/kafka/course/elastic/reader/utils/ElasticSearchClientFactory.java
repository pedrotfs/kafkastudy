package com.github.pedrosilva.kafka.course.elastic.reader.utils;

import com.github.pedrosilva.kafka.course.elastic.reader.config.callback.ElasticSearchClientConfigCallback;
import com.github.pedrosilva.kafka.course.util.PropertyLoader;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Properties;

public class ElasticSearchClientFactory {

    private static final String SCHEMA = "https";

    public static RestHighLevelClient createClient() throws IOException {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        final Properties configurations = PropertyLoader.getConfigurations();
        final String username = configurations.getProperty("elastic.username");
        final String password = configurations.getProperty("elastic.password");
        final String hostname = configurations.getProperty("elastic.hostname");
        final String hostport = configurations.getProperty("elastic.hostport");

        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        RestClientBuilder builder =
                RestClient.builder(new HttpHost(hostname, Integer.valueOf(hostport), SCHEMA))
                .setHttpClientConfigCallback(new ElasticSearchClientConfigCallback(credentialsProvider));

        return new RestHighLevelClient(builder);

    }
}
