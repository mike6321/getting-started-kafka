package org.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

public class SimpleProducerASync {

    private static final Logger logger = LoggerFactory.getLogger(SimpleProducerASync.class);

    public static void main(String[] args) {
        String topicName = "simple-topic";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "id-001", "hello world2!");

        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                if (Objects.isNull(exception)) {
                    logger.info("\n ########## record meta data received ########## \n" +
                            "partition: " + recordMetadata.partition() + "\n" +
                            "offset:" + recordMetadata.offset() + "\n" +
                            "timestamp:" + recordMetadata.timestamp() + "\n");
                } else {
                    logger.error("exception error from broker :: " + exception.getMessage());
                }
            }
        });

        try {
            Thread.sleep(3000l);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();
    }

}
