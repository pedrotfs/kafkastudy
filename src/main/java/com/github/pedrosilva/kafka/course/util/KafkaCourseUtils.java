package com.github.pedrosilva.kafka.course.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaCourseUtils {

    private static final String DEFAULT_BOOTSTRAP_SERVER_VALUE = "127.0.0.1";

    private static final String DEFAULT_BOOTSTRAP_SERVER_PORT = "9092";

    private static final String DEFAULT_GROUP = "my-first-app";

    private static final String OFFSET_RESET_VALUE = "earliest"; //or latest or none

    private static final String COMPRESSION = "snappy";

    private static final int BATCH_SIZE = 32 * 1024;

    public static Properties createPropertiesProducer() {
        return createPropertiesProducer(DEFAULT_BOOTSTRAP_SERVER_VALUE);
    }

    public static Properties createPropertiesProducer(final String bootstrapServer) {
        return createPropertiesProducer(bootstrapServer, DEFAULT_BOOTSTRAP_SERVER_PORT);
    }

    public static Properties createPropertiesProducer(final String bootstrapServer, final String port) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer + ":" + port);
        setSerializerPropertiesProducer(properties);
        return properties;
    }

    public static Properties createPropertiesConsumer() {
        return createPropertiesConsumer(DEFAULT_GROUP);
    }

    public static Properties createPropertiesConsumer(final String groupId) {
        return createPropertiesConsumer(groupId, DEFAULT_BOOTSTRAP_SERVER_VALUE);
    }

    public static Properties createPropertiesConsumer(final String groupId, final String bootstrapServer) {
        return createPropertiesConsumer(groupId, bootstrapServer, DEFAULT_BOOTSTRAP_SERVER_PORT);
    }

    public static Properties createPropertiesConsumer(final String groupId, final String bootstrapServer, final String port) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer + ":" + port);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_VALUE);
        setSerializerPropertiesConsumer(properties);
        return properties;
    }

    public static Properties createPropertiesConsumerWithoutGroup() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVER_VALUE + ":" + DEFAULT_BOOTSTRAP_SERVER_PORT);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_VALUE);
        setSerializerPropertiesConsumer(properties);
        return properties;
    }

    private static void setSerializerPropertiesProducer(Properties properties) {
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //safety
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        //high throughput producer with more cost to cpu and latency
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION);
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(BATCH_SIZE));
    }

    private static void setSerializerPropertiesConsumer(Properties properties) {
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    }
}
