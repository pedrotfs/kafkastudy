package com.github.pedrosilva.kafka.course.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerFactory {

    private static final String DEFAULT_BOOTSTRAP_SERVER_VALUE = "127.0.0.1";

    private static final String DEFAULT_BOOTSTRAP_SERVER_PORT = "9092";

    private static final String DEFAULT_TOPIC = "first_topic";

    public Properties createProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVER_VALUE + ":" + DEFAULT_BOOTSTRAP_SERVER_PORT);
        setSerializerProperties(properties);
        return properties;
    }

    public Properties createProperties(final String bootstrapServer) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer + ":" + DEFAULT_BOOTSTRAP_SERVER_PORT);
        setSerializerProperties(properties);
        return properties;
    }

    public Properties createProperties(final String bootstrapServer, final String port) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer + ":" + port);
        setSerializerProperties(properties);
        return properties;
    }

    private void setSerializerProperties(Properties properties)
    {
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    public KafkaProducer<String, String> createProducer()
    {
        return new KafkaProducer<String, String>(createProperties());
    }

    public KafkaProducer<String, String> createProducer(final String boostrapServer)
    {
        return new KafkaProducer<String, String>(createProperties(boostrapServer));
    }

    public KafkaProducer<String, String> createProducer(final String boostrapServer, final String port)
    {
        return new KafkaProducer<String, String>(createProperties(boostrapServer, port));
    }

    public ProducerRecord<String, String> createProducerRecord(final String value)
    {
        return new ProducerRecord<String, String>(DEFAULT_TOPIC, value);
    }

    public ProducerRecord<String, String> createProducerRecord(final String value, final String topic)
    {
        return new ProducerRecord<String, String>(topic, value);
    }

}
