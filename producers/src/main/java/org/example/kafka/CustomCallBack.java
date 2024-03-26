package org.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class CustomCallBack implements Callback {

    private static final Logger logger = LoggerFactory.getLogger(CustomCallBack.class);
    private int seq;

    public CustomCallBack(int seq) {
        this.seq = seq;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
        if (Objects.isNull(exception)) {
            logger.info("seq {} partition: {} offset {}", this.seq, recordMetadata.partition(), recordMetadata.offset());
        } else {
            logger.error("exception error from broker :: " + exception.getMessage());
        }
    }

}
