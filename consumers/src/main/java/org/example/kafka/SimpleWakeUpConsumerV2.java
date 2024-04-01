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

public class SimpleWakeUpConsumerV2 {

    private static final Logger logger = LoggerFactory.getLogger(SimpleWakeUpConsumerV2.class);

    /**
     * 60초가 넘어가는 시점
     * 리밸런싱 수행
     * [Consumer]
     * [main] INFO org.example.kafka.SimpleWakeUpConsumerV2 - main thread is sleeping 70000 ms during while loop
     * [kafka-coordinator-heartbeat-thread | group-02] WARN org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-group-02-1, groupId=group-02] consumer poll timeout has expired. This means the time between subsequent calls to poll() was longer than the configured max.poll.interval.ms, which typically implies that the poll loop is spending too much time processing messages. You can address this either by increasing max.poll.interval.ms or by reducing the maximum size of batches returned in poll() with max.poll.records.
     * [kafka-coordinator-heartbeat-thread | group-02] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-group-02-1, groupId=group-02] Member consumer-group-02-1-a23536bd-28f5-43cb-8bb4-b3379e60b29b sending LeaveGroup request to coordinator localhost:9092 (id: 2147483647 rack: null) due to consumer poll timeout has expired.
     * [kafka-coordinator-heartbeat-thread | group-02] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-group-02-1, groupId=group-02] Resetting generation due to: consumer pro-actively leaving the group
     * [kafka-coordinator-heartbeat-thread | group-02] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-group-02-1, groupId=group-02] Request joining group due to: consumer pro-actively leaving the group
     *
     * [Broker]
     * [2024-04-01 22:31:43,632] INFO [GroupCoordinator 0]: Preparing to rebalance group group-02 in state PreparingRebalance with old generation 1 (__consumer_offsets-46) (reason: Removing member consumer-group-02-1-a23536bd-28f5-43cb-8bb4-b3379e60b29b on LeaveGroup) (kafka.coordinator.group.GroupCoordinator)
     * [2024-04-01 22:31:43,633] INFO [GroupCoordinator 0]: Group group-02 with generation 2 is now empty (__consumer_offsets-46) (kafka.coordinator.group.GroupCoordinator)
     * [2024-04-01 22:31:43,634] INFO [GroupCoordinator 0]: Member MemberMetadata(memberId=consumer-group-02-1-a23536bd-28f5-43cb-8bb4-b3379e60b29b, groupInstanceId=None, clientId=consumer-group-02-1, clientHost=/127.0.0.1, sessionTimeoutMs=45000, rebalanceTimeoutMs=60000, supportedProtocols=List(range, cooperative-sticky)) has left group group-02 through explicit `LeaveGroup` request (kafka.coordinator.group.GroupCoordinator)
     * [2024-04-01 22:31:53,595] INFO [GroupCoordinator 0]: Dynamic member with unknown member id joins group group-02 in Empty state. Created a new member id consumer-group-02-1-d36d04fa-c812-4e69-b080-cf8cfa2e601c and request the member to rejoin with this id. (kafka.coordinator.group.GroupCoordinator)
     * [2024-04-01 22:31:53,600] INFO [GroupCoordinator 0]: Preparing to rebalance group group-02 in state PreparingRebalance with old generation 2 (__consumer_offsets-46) (reason: Adding new member consumer-group-02-1-d36d04fa-c812-4e69-b080-cf8cfa2e601c with group instance id None) (kafka.coordinator.group.GroupCoordinator)
     * [2024-04-01 22:31:53,601] INFO [GroupCoordinator 0]: Stabilized group group-02 generation 3 (__consumer_offsets-46) with 1 members (kafka.coordinator.group.GroupCoordinator)
     * [2024-04-01 22:31:53,605] INFO [GroupCoordinator 0]: Assignment received from leader consumer-group-02-1-d36d04fa-c812-4e69-b080-cf8cfa2e601c for group group-02 for generation 3. The group has 1 members, 0 of which are static. (kafka.coordinator.group.GroupCoordinator)
     * */
    public static void main(String[] args) {
        String topicName ="pizza-topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-02");
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");

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

        int loopCount = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info("******************** loopCount: {}, consumerRecordCount: {} ********************", loopCount++, consumerRecords.count());
                for (ConsumerRecord record : consumerRecords) {
                    logger.info("key: {}, value: {}, partition: {}, offset: {}", record.key(), record.value(), record.partition(), record.offset());
                }

                try {
                    logger.info("main thread is sleeping {} ms during while loop", loopCount * 10000);
                    Thread.sleep(loopCount * 10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
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
