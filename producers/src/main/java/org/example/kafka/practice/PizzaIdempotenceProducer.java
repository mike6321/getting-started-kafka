package org.example.kafka.practice;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
토픽 생성 
./kafka-topics --bootstrap-server localhost:9092 --create --topic pizza-topic --partitions 3
켠슈머
./kafka-console-consumer --bootstrap-server localhost:9092 --topic pizza-topic c --group group_01 \
--property print.key=true \
--property print.value=true \
--property print.partition=true
*/
public class PizzaIdempotenceProducer {

    private static final Logger logger = LoggerFactory.getLogger(PizzaIdempotenceProducer.class);

    public static void main(String[] args) {
        String topicName = "pizza-topic";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Idempotence로 가지 않는다.
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "6");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "0");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // Exception in thread "main" org.apache.kafka.common.config.ConfigException: Must set acks to all in order to use the idempotent producer. Otherwise we cannot guarantee idempotence.

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        sendPizzaMessage(kafkaProducer, topicName, -1, 100, 1000, 100, false);
        kafkaProducer.close();
    }

    public static void sendPizzaMessage(KafkaProducer<String, String> kafkaProducer,
                                        String topicName,
                                        int iterCount,
                                        int interIntervalMillis,
                                        int intervalMillis,
                                        int intervalCount,
                                        boolean sync) {
        PizzaMessage pizzaMessage = new PizzaMessage();
        int iterSeq = 0;
        long seed = 2022;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        while (iterSeq != iterCount) {
            HashMap<String, String> pMessage = pizzaMessage.produce_msg(faker, random, iterSeq);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                    topicName, pMessage.get("key"),
                    pMessage.get("message")
            );

            sendMessage(kafkaProducer, producerRecord, pMessage, sync);

            if (intervalCount > 0 && iterSeq % intervalCount == 0) {
                try {
                    logger.info("######### IntervalCount: " + intervalCount +
                            " intervalMillis: " + intervalMillis + " #########");
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

            if (interIntervalMillis > 0) {
                try {
                    logger.info("interIntervalMillis: " + interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
            iterSeq++;
        }
    }

    public static void sendMessage(KafkaProducer<String, String> kafkaProducer,
                                   ProducerRecord<String, String> producerRecord,
                                   Map<String, String> pMssage,
                                   boolean sync) {
        if (!sync) {
            kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
                if (Objects.isNull(exception)) {
                    logger.info("async message: " + pMssage.get("key") +
                            " partitions: " + recordMetadata.partition() +
                            " offset: " + recordMetadata.offset());
                } else {
                    logger.error("exception error from broker :: " + exception.getMessage());
                }
            });

            return;
        }

        try {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            logger.info("sync message: " + pMssage.get("key") +
                    " partitions: " + recordMetadata.partition() +
                    " offset: " + recordMetadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}
