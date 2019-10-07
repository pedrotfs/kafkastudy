package com.github.pedrosilva.kafka.course.start;

import com.github.pedrosilva.kafka.course.producer.KafkaProducerFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartApplication {

    private static Logger LOG = LoggerFactory.getLogger(StartApplication.class);

    private static final String MESSAGE = "iterating new callbacksss";

    private static final int ITERATIONS = 10;

    private static final int KEY_FACTOR = 5; //same key same partition

    public static void main(String[] args)
    {
        KafkaProducerFactory factory = new KafkaProducerFactory();
        KafkaProducer<String, String> producer = factory.createProducer();

        LOG.info("beginning.");
//        iterateWithoutKeys(factory, producer);
        iterateWithKeys(factory, producer);

        producer.flush();
        producer.close();
        LOG.info("ending.");
    }

    private static void iterateWithKeys(KafkaProducerFactory factory, KafkaProducer<String, String> producer) {
        for(int i = 0; i < ITERATIONS; i ++)
        {
            ProducerRecord<String, String> record = factory.createProducerRecord(MESSAGE + " with keys - " + i, "KEY" + i % KEY_FACTOR);
            LOG.info("************************sending message with key: " + record.value());
            producer.send(record, factory.getProducerCallBack());
            LOG.info("************************message with key sent: " + record.value());
        }
    }

    private static void iterateWithoutKeys(KafkaProducerFactory factory, KafkaProducer<String, String> producer) {
        for(int i = 0; i < ITERATIONS; i ++)
        {
            ProducerRecord<String, String> record = factory.createProducerRecord(MESSAGE + " - " + i);
            LOG.info("************************sending message: " + record.value());
            producer.send(record, factory.getProducerCallBack());
            LOG.info("************************message sent: " + record.value());
        }
    }
}
