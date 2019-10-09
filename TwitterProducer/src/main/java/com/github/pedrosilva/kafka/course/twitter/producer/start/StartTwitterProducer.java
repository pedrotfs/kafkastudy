package com.github.pedrosilva.kafka.course.twitter.producer.start;


import com.github.pedrosilva.kafka.course.producer.KafkaProducerFactory;
import com.github.pedrosilva.kafka.course.twitter.producer.utils.HoseBirdClientUtil;
import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class StartTwitterProducer {

    private static final int CAPACITY = 100000;

    private static final String TOPIC = "tweets"; //please create the topic first 6p 3r

    private static Logger LOG = LoggerFactory.getLogger(StartTwitterProducer.class);

    private StartTwitterProducer() {
        //Ok
    }

    public static void main(String[] args) {
        new StartTwitterProducer().run();
    }

    private void run() {
        //Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>(CAPACITY);

        LOG.info("Starting. Creating client.");
        Client client;
        try {
            client = HoseBirdClientUtil.createHBCClient(messageQueue, getSearchTermList());
        } catch (IOException e) {
            LOG.error("error reading config", e);
            return;
        }
        LOG.info("Attempting to connect");
        client.connect();

        KafkaProducerFactory producerFactory = new KafkaProducerFactory();
        KafkaProducer<String, String> producer = producerFactory.createProducer();


        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String message = null;
            try {
                message = messageQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(message != null)
            {
                handleMessage(message, producer, producerFactory.createProducerRecord(message, null, TOPIC), producerFactory.getProducerCallBack());
            }
        }
        client.stop();
        producer.close();
    }

    private void handleMessage(String message, KafkaProducer<String, String> producer, ProducerRecord<String, String> record, Callback callBack) {
        LOG.info(message);
        producer.send(record, callBack);
    }

    private List<String> getSearchTermList()
    {
        List<String> list = new ArrayList<>();
        list.add("beatles");
        return list;
    }
}
