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

[2024-03-26 19:43:29,658] INFO [GroupCoordinator 0]: Dynamic member with unknown member id joins group group_01 in Stable state. Created a new member id console-consumer-ad4ece92-df31-4316-b9bc-c617af245a17 and request the member to rejoin with this id. (kafka.coordinator.group.GroupCoordinator)
[2024-03-26 19:43:29,660] INFO [GroupCoordinator 0]: Preparing to rebalance group group_01 in state PreparingRebalance with old generation 2 (__consumer_offsets-45) (reason: Adding new member console-consumer-ad4ece92-df31-4316-b9bc-c617af245a17 with group instance id None) (kafka.coordinator.group.GroupCoordinator)
[2024-03-26 19:43:30,961] INFO [GroupCoordinator 0]: Stabilized group group_01 generation 3 (__consumer_offsets-45) with 3 members (kafka.coordinator.group.GroupCoordinator)
[2024-03-26 19:43:30,962] INFO [GroupCoordinator 0]: Assignment received from leader console-consumer-b5938c29-313e-4b56-abbd-e8266ea8d86b for group group_01 for generation 3. The group has 3 members, 0 of which are static. (kafka.coordinator.group.GroupCoordinator)
*/
public class PizzaProducer {

    private static final Logger logger = LoggerFactory.getLogger(PizzaProducer.class);

    public static void main(String[] args) {
        String topicName = "pizza-topic";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        sendPizzaMessage(kafkaProducer, topicName, -1, 100, 1000, 100, true);
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
