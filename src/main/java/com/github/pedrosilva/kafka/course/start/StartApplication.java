package com.github.pedrosilva.kafka.course.start;

import com.github.pedrosilva.kafka.course.producer.KafkaProducerFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class StartApplication {

    public static void main(String[] args)
    {
        KafkaProducerFactory factory = new KafkaProducerFactory();
        KafkaProducer<String, String> producer = factory.createProducer();
        ProducerRecord<String, String> record = factory.createProducerRecord("fromJava");

        System.out.println("Beginning.");
        producer.send(record);
        producer.flush();
        producer.close();
        System.out.println("Ending.");
    }
}
