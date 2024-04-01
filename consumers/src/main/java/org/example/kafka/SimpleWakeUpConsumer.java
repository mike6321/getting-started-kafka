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

        /**
         * ./kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group group-01-static
         * GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                            HOST            CLIENT-ID
         * group-01-static pizza-topic     1          9               9               0               2-525d6297-8d48-4c65-b3a4-d70c685321c9 /127.0.0.1      consumer-group-01-static-2
         * group-01-static pizza-topic     0          10              10              0               1-41f1f201-fcc0-4d12-a9ed-eeeb467ae0bf /127.0.0.1      consumer-group-01-static-1
         * group-01-static pizza-topic     2          5               5               0               3-2d109f1e-0cdc-4943-8881-8d98c0e2eea2 /127.0.0.1      consumer-group-01-static-3
         *
         * 45초 내에 3번 컨슈머 재기동
         *
         * GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                            HOST            CLIENT-ID
         * group-01-static pizza-topic     1          68              68              0               2-525d6297-8d48-4c65-b3a4-d70c685321c9 /127.0.0.1      consumer-group-01-static-2
         * group-01-static pizza-topic     0          66              67              1               1-41f1f201-fcc0-4d12-a9ed-eeeb467ae0bf /127.0.0.1      consumer-group-01-static-1
         * group-01-static pizza-topic     2          52              54              2               3-2060655c-fa61-44b9-81e2-f357697734c2 /127.0.0.1      consumer-group-01-static-3
         *
         * 45초 이후에 3번 컨슈머 재기동
         * 
         * 리밸런싱 발생
         * [2024-04-01 21:24:34,371] INFO [GroupCoordinator 0]: Preparing to rebalance group group-01-static in state PreparingRebalance with old generation 3 (__consumer_offsets-46) (reason: removing member 3-2060655c-fa61-44b9-81e2-f357697734c2 on heartbeat expiration) (kafka.coordinator.group.GroupCoordinator)
         * [2024-04-01 21:24:36,567] INFO [GroupCoordinator 0]: Stabilized group group-01-static generation 4 (__consumer_offsets-46) with 2 members (kafka.coordinator.group.GroupCoordinator)
         * [2024-04-01 21:24:36,568] INFO [GroupCoordinator 0]: Assignment received from leader 1-41f1f201-fcc0-4d12-a9ed-eeeb467ae0bf for group group-01-static for generation 4. The group has 2 members, 2 of which are static. (kafka.coordinator.group.GroupCoordinator)
         * */
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-01-static");
//        properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1");
//        properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "2");
        properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "3");

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
