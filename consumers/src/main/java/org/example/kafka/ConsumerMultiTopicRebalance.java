package org.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerMultiTopicRebalance {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerMultiTopicRebalance.class);

    /**
     * ./kafka-topics --bootstrap-server localhost:9092 --create --topic topic-p3-t1 --partitions 3
     * ./kafka-topics --bootstrap-server localhost:9092 --create --topic topic-p3-t2 --partitions 3
     *
     * [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-group-m-topic-1, groupId=group-m-topic] Found no committed offset for partition topic-p3-t2-1
     * [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-group-m-topic-1, groupId=group-m-topic] Found no committed offset for partition topic-p3-t1-0
     * [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-group-m-topic-1, groupId=group-m-topic] Found no committed offset for partition topic-p3-t2-0
     * [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-group-m-topic-1, groupId=group-m-topic] Found no committed offset for partition topic-p3-t1-2
     * [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-group-m-topic-1, groupId=group-m-topic] Found no committed offset for partition topic-p3-t2-2
     * [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-group-m-topic-1, groupId=group-m-topic] Found no committed offset for partition topic-p3-t1-1
     *
     * ./kafka-console-producer --bootstrap-server localhost:9092 --topic topic-p3-t1
     * ./kafka-console-producer --bootstrap-server localhost:9092 --topic topic-p3-t2
     * */
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-m-topic");
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(List.of("topic-p3-t1", "topic-p3-t2"));

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("main program starts to exit by calling wakeup");
            kafkaConsumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord record : consumerRecords) {
                    logger.info("topic: {}, key: {}, value: {}, partition: {}, offset: {}", record.topic(), record.key(), record.value(), record.partition(), record.offset());
                }

            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } finally {
            logger.info("finnaly consumer is closing");
            kafkaConsumer.close();
        }


    }

}
