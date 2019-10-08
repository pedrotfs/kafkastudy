package com.github.pedrosilva.kafka.course.elastic.reader.config.callback;

import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClientBuilder;

public class ElasticSearchClientConfigCallback implements RestClientBuilder.HttpClientConfigCallback {

    private CredentialsProvider credentialsProvider;

    public ElasticSearchClientConfigCallback(CredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
    }

    @Override
    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
    }
}
