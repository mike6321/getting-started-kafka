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
./kafka-topics --bootstrap-server localhost:9092 --create --topic pizza-topic-partitioner --partitions 5
켠슈머
./kafka-dump-log --deep-iteration --files ../../data/kafka-logs/pizza-topic-partitioner-0/00000000000000000000.log --print-data-log
./kafka-dump-log --deep-iteration --files ../../data/kafka-logs/pizza-topic-partitioner-1/00000000000000000000.log --print-data-log

./kafka-dump-log --deep-iteration --files ../../data/kafka-logs/pizza-topic-partitioner-2/00000000000000000000.log --print-data-log | grep P001
*/
public class PizzaCustomPartitionerProducer {

    private static final Logger logger = LoggerFactory.getLogger(PizzaCustomPartitionerProducer.class);

    public static void main(String[] args) {
        String topicName = "pizza-topic-partitioner";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.example.kafka.practice.CustomPartitioner");
        properties.setProperty("custom.specialKey", "P001");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        sendPizzaMessage(kafkaProducer, topicName, -1, 100, 0, 0, false);
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
