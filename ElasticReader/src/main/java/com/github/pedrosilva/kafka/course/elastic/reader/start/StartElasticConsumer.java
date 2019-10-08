package com.github.pedrosilva.kafka.course.elastic.reader.start;

import com.github.pedrosilva.kafka.course.elastic.reader.utils.ElasticSearchClientFactory;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class StartElasticConsumer {

    private Logger LOG = LoggerFactory.getLogger(StartElasticConsumer.class);

    private static final String INDEX = "twitter";

    private static final String INDEX_TYPE = "tweets";

    private StartElasticConsumer() {
        //OK
    }

    public static void main(String[] args) {
        new StartElasticConsumer().run();
    }

    private void run()
    {
        try {
            RestHighLevelClient client = ElasticSearchClientFactory.createClient();

            final String json = "{\"property\":\"value\"}";

            IndexRequest indexRequest = new IndexRequest(INDEX, INDEX_TYPE).source(json, XContentType.JSON);
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

            LOG.info(indexResponse.getId());
            client.close();

        } catch (IOException e) {
            LOG.error("can't connect to elastic search cloud.");
        }
    }
}
