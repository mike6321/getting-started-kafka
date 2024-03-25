package org.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

/**
토픽 생성 
./kafka-topics --bootstrap-server localhost:9092 --create --topic multipart-topic --partitions 3
켠슈머
./kafka-console-consumer --bootstrap-server localhost:9092 --topic multipart-topic \
--property print.key=true \
--property print.value=true \
*/
public class ProducerASyncWithKey {

    private static final Logger logger = LoggerFactory.getLogger(ProducerASyncWithKey.class);

    public static void main(String[] args) {
        String topicName = "multipart-topic";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        for (int seq = 0; seq < 20; seq++) {
            logger.info("seg : {}", seq);

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, String.valueOf(seq), "hello world" + seq);

            kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
                if (Objects.isNull(exception)) {
                    logger.info("\n ########## record meta data received ########## \n" +
                            "partition: " + recordMetadata.partition() + "\n" +
                            "offset:" + recordMetadata.offset() + "\n" +
                            "timestamp:" + recordMetadata.timestamp() + "\n");
                } else {
                    logger.error("exception error from broker :: " + exception.getMessage());
                }
            });
        }

        try {
            Thread.sleep(3000l);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();
    }

}
