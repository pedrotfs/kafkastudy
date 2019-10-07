package com.github.pedrosilva.kafka.course.start;


import com.github.pedrosilva.kafka.course.util.HoseBirdClientUtil;
import com.twitter.hbc.core.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class StartTwitterProducer {

    private static final int CAPACITY = 100000;

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
        Client client = HoseBirdClientUtil.createHBCClient(messageQueue, getSearchTermList());
        LOG.info("Attempting to connect");
        client.connect();

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
                LOG.info(message);
            }
        }
    }

    private List<String> getSearchTermList()
    {
        List<String> list = new ArrayList<>();
        list.add("petr4");
        return list;
    }
}
