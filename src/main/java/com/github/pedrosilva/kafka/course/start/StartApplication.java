package com.github.pedrosilva.kafka.course.start;

import com.github.pedrosilva.kafka.course.consumer.KafkaConsumerFactory;
import com.github.pedrosilva.kafka.course.producer.KafkaProducerFactory;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class StartApplication {

    private static Logger LOG = LoggerFactory.getLogger(StartApplication.class);

    private static final String MESSAGE = "iterating new callbacksss";

    private static final int ITERATIONS = 10;

    private static final int KEY_FACTOR = 5; //same key same partition

    private static final long MILLI_SECONDS_POLL = 10000;

    public static void main(String[] args)
    {
        KafkaProducerFactory producerFactory = new KafkaProducerFactory();
        KafkaProducer<String, String> producer = producerFactory.createProducer();

        messageProduction(producerFactory, producer);
        messageConsuption();
    }

    private static void messageConsuption() {
        LOG.info("begin consuming.");
        final KafkaConsumerFactory consumerFactory = new KafkaConsumerFactory();
//        consumeAssingSeek(consumerFactory); //consume assign seek
        consume(consumerFactory);
        LOG.info("end consuming.");
    }

    private static void consumeAssingSeek(KafkaConsumerFactory consumerFactory) {
        final KafkaConsumer<String, String> consumerAssignSeek = consumerFactory.createConsumerAssignSeek(0, null, 10);
        ConsumerRecords<String, String> records = consumerAssignSeek.poll(Duration.ofMillis(MILLI_SECONDS_POLL));
        records.forEach(r -> {
            LOG.info("Key: " + r.key() + " - Val: " + r.value());
            LOG.info("Prt: " + r.partition() + " - Tms: " + r.timestamp() + " - Off: " + r.offset());
        });
        consumerAssignSeek.close();
    }

    private static void consume(KafkaConsumerFactory consumerFactory) {
        final KafkaConsumer<String, String> consumer = consumerFactory.createConsumer();
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(MILLI_SECONDS_POLL));
        records.forEach(r -> {
            LOG.info("Key: " + r.key() + " - Val: " + r.value());
            LOG.info("Prt: " + r.partition() + " - Tms: " + r.timestamp() + " - Off: " + r.offset());
        });
        consumer.close();
    }

    private static void messageProduction(KafkaProducerFactory factory, KafkaProducer<String, String> producer) {
        LOG.info("beginning production.");
//        iterateWithoutKeys(factory, producer);
        iterateWithKeys(factory, producer);
        producer.flush();
        producer.close();
        LOG.info("ending production.");
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
