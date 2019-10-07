package com.github.pedrosilva.kafka.course.start;

import com.github.pedrosilva.kafka.course.producer.KafkaProducerFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartApplication {

    private static Logger LOG = LoggerFactory.getLogger(StartApplication.class);

    private static final String MESSAGE = "iterating new callbacks";

    private static final int ITERATIONS = 10;

    public static void main(String[] args)
    {
        KafkaProducerFactory factory = new KafkaProducerFactory();
        KafkaProducer<String, String> producer = factory.createProducer();
        LOG.info("beginning.");
        for(int i = 0; i < ITERATIONS; i ++)
        {
            ProducerRecord<String, String> record = factory.createProducerRecord(MESSAGE + " - " + i);
            LOG.info("************************sending message: " + record.value());
            producer.send(record, factory.getProducerCallBack());
            LOG.info("************************message sent: " + record.value());
        }
        producer.flush();
        producer.close();
        LOG.info("ending.");
    }
}
