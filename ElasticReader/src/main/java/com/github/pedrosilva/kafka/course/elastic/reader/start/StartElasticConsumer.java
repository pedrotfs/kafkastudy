package com.github.pedrosilva.kafka.course.elastic.reader.start;

import com.github.pedrosilva.kafka.course.consumer.KafkaConsumerFactory;
import com.github.pedrosilva.kafka.course.elastic.reader.utils.ElasticSearchClientFactory;
import com.github.pedrosilva.kafka.course.util.KafkaCourseUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

public class StartElasticConsumer {

    private Logger LOG = LoggerFactory.getLogger(StartElasticConsumer.class);

    private static final String INDEX = "twitter";

    private static final String INDEX_TYPE = "tweets";

    private static final String CONSUMER_GROUP = "elasticsearch-tweets";

    private static final String CONSUMER_TOPIC = "tweets";

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
            KafkaConsumerFactory consumerFactory = new KafkaConsumerFactory();

            final KafkaConsumer<String, String> consumer = consumerFactory.createConsumer(CONSUMER_GROUP,
                    KafkaCourseUtils.DEFAULT_BOOTSTRAP_SERVER_VALUE, KafkaCourseUtils.DEFAULT_BOOTSTRAP_SERVER_PORT, CONSUMER_TOPIC);

            consumeKafkaFeed(consumer, client);
            consumer.close();
            client.close();

        } catch (IOException e) {
            LOG.error("can't connect to elastic search cloud.");
        }
    }

    private void consumeKafkaFeed(KafkaConsumer<String, String> consumer, RestHighLevelClient client) {
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            records.forEach(r -> {
                LOG.info("Key: " + r.key() + " - Val: " + r.value());
                LOG.info("Prt: " + r.partition() + " - Tms: " + r.timestamp() + " - Off: " + r.offset());

                IndexRequest indexRequest = new IndexRequest(INDEX, INDEX_TYPE).source(r.value(), XContentType.JSON);
                IndexResponse indexResponse = null;
                try {
                    indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                    LOG.info("response id : " + indexResponse.getId());
                } catch (IOException e) {
                    LOG.error("why cant I just throw this?");
                } catch (ElasticsearchException e)
                {
                    LOG.info("Key: " + r.key() + " - Val: " + r.value() + "has given an exception indexing!!!!!!!!!!!");
                }
            });
        }
    }
}
