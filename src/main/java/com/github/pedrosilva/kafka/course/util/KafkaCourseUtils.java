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

    private static void setSerializerPropertiesProducer(Properties properties) {
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    private static void setSerializerPropertiesConsumer(Properties properties) {
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    }
}
