package com.github.pedrosilva.kafka.course.consumer;

import com.github.pedrosilva.kafka.course.util.KafkaCourseUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerFactory {

    private static final String DEFAULT_TOPIC = "first_topic";


    public Properties createProperties() {
        return KafkaCourseUtils.createPropertiesConsumer();
    }

    public Properties createProperties(final String groupId) {
        return KafkaCourseUtils.createPropertiesConsumer(groupId);
    }

    public Properties createProperties(final String groupId, final String bootstrapServer) {
        return KafkaCourseUtils.createPropertiesConsumer(bootstrapServer, bootstrapServer);
    }

    public Properties createProperties(final String groupId, final String bootstrapServer, final String port) {
        return KafkaCourseUtils.createPropertiesConsumer(bootstrapServer, port);
    }

    public KafkaConsumer<String, String> createConsumer()
    {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(createProperties());
        subscribeToDefaultTopic(consumer, DEFAULT_TOPIC);
        return consumer;
    }

    public KafkaConsumer<String, String> createConsumer(final String groupId)
    {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(createProperties(groupId));
        subscribeToDefaultTopic(consumer, DEFAULT_TOPIC);
        return consumer;
    }

    public KafkaConsumer<String, String> createConsumer(final String groupId, final String boostrapServer)
    {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(createProperties(groupId, boostrapServer));
        subscribeToDefaultTopic(consumer, DEFAULT_TOPIC);
        return consumer;
    }

    public KafkaConsumer<String, String> createConsumer(final String groupId, final String boostrapServer, final String port)
    {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(createProperties(groupId, boostrapServer, port));
        subscribeToDefaultTopic(consumer, DEFAULT_TOPIC);
        return consumer;
    }

    public KafkaConsumer<String, String> createConsumer(final String groupId, final String boostrapServer, final String port, final String topic)
    {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(createProperties(groupId, boostrapServer, port));
        subscribeToDefaultTopic(consumer, topic);
        return consumer;
    }

    private void subscribeToDefaultTopic(KafkaConsumer<String, String> consumer, final String topic) {
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaConsumer<String, String> createConsumerAssignSeek(final int partition, final String topic, long offset)
    {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KafkaCourseUtils.createPropertiesConsumerWithoutGroup());
        String topicUsed = null;
        topicUsed = topic != null ? topic : DEFAULT_TOPIC;
        TopicPartition topicPartition = new TopicPartition(topicUsed, partition);
        consumer.assign(Collections.singletonList(topicPartition));
        consumer.seek(topicPartition, offset);
        return consumer;
    }
}
