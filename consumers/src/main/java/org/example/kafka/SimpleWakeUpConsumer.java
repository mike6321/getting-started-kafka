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

/**
 * pro-actively leaving the group
 *
 * rebalancing
 * [2024-04-01 20:32:21,586] INFO [GroupCoordinator 0]: Assignment received from leader consumer-group-01-1-18d0c1d9-0db3-468e-8a51-6849af39ad0e for group group-01 for generation 10. The group has 1 members, 0 of which are static. (kafka.coordinator.group.GroupCoordinator)
 * 2024-04-01 20:33:54,602] INFO [GroupCoordinator 0]: Assignment received from leader consumer-group-01-1-18d0c1d9-0db3-468e-8a51-6849af39ad0e for group group-01 for generation 11. The group has 2 members, 0 of which are static. (kafka.coordinator.group.GroupCoordinator)
 * [2024-04-01 20:35:03,622] INFO [GroupCoordinator 0]: Assignment received from leader consumer-group-01-1-18d0c1d9-0db3-468e-8a51-6849af39ad0e for group group-01 for generation 12. The group has 3 members, 0 of which are static. (kafka.coordinator.group.GroupCoordinator)
 * */
public class SimpleWakeUpConsumer {

    private static final Logger logger = LoggerFactory.getLogger(SimpleWakeUpConsumer.class);

    public static void main(String[] args) {
        String topicName ="pizza-topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-01");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(List.of(topicName));

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
                    logger.info("key: {}, value: {}, partition: {}, offset: {}", record.key(), record.value(), record.partition(), record.offset());
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
