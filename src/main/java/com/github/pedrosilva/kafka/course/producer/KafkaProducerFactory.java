package com.github.pedrosilva.kafka.course.producer;

import com.github.pedrosilva.kafka.course.producer.callback.ProducerCallBack;
import com.github.pedrosilva.kafka.course.util.KafkaCourseUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerFactory {

    private static final String DEFAULT_TOPIC = "first_topic";

    public Properties createProperties() {
        return KafkaCourseUtils.createPropertiesProducer();
    }

    public Properties createProperties(final String bootstrapServer) {
       return KafkaCourseUtils.createPropertiesProducer(bootstrapServer);
    }

    public Properties createProperties(final String bootstrapServer, final String port) {
        return KafkaCourseUtils.createPropertiesProducer(bootstrapServer, port);
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

    public ProducerRecord<String, String> createProducerRecord(final String value, final String key)
    {
        return new ProducerRecord<String, String>(DEFAULT_TOPIC, key, value);
    }

    public ProducerRecord<String, String> createProducerRecord(final String value, final String key, final String topic)
    {
        return new ProducerRecord<String, String>(topic, key, value);
    }

    public Callback getProducerCallBack()
    {
        return new ProducerCallBack();
    }

}
