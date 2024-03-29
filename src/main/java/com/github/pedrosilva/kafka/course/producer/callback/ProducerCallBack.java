package com.github.pedrosilva.kafka.course.producer.callback;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerCallBack implements Callback {

    private static Logger LOG = LoggerFactory.getLogger(ProducerCallBack.class);

    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        LOG.info("topic: " + recordMetadata.topic() + " - Prt: " + recordMetadata.partition() + " - Tms: " + recordMetadata.timestamp() + " - Off: " + recordMetadata.offset());
        if(e != null)
        {
            LOG.error("something went wrong.");
            e.printStackTrace();
            return;
        }
        LOG.info("message sent");
    }
}
