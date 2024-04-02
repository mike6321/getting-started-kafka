package org.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumeAssign {

    private static final Logger logger = LoggerFactory.getLogger(ConsumeAssign.class);

    /**
     * default assign strategy : RangeAssignor
     * partition.assignment.strategy = [class org.apache.kafka.clients.consumer.RangeAssignor, class org.apache.kafka.clients.consumer.CooperativeStickyAssignor]
     *
     * [Broker]
     * GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                                  HOST            CLIENT-ID
     * group-assign    topic-p3-t2     1          1               1               0               consumer-group-assign-1-d62a438e-93a9-4919-9eb3-1fea7f39be35 /127.0.0.1      consumer-group-assign-1
     * group-assign    topic-p3-t1     1          1               1               0               consumer-group-assign-1-d62a438e-93a9-4919-9eb3-1fea7f39be35 /127.0.0.1      consumer-group-assign-1
     * group-assign    topic-p3-t2     2          2               2               0               consumer-group-assign-1-d62a438e-93a9-4919-9eb3-1fea7f39be35 /127.0.0.1      consumer-group-assign-1
     * group-assign    topic-p3-t2     0          0               0               0               consumer-group-assign-1-d62a438e-93a9-4919-9eb3-1fea7f39be35 /127.0.0.1      consumer-group-assign-1
     * group-assign    topic-p3-t1     2          2               2               0               consumer-group-assign-1-d62a438e-93a9-4919-9eb3-1fea7f39be35 /127.0.0.1      consumer-group-assign-1
     * group-assign    topic-p3-t1     0          0               0               0               consumer-group-assign-1-d62a438e-93a9-4919-9eb3-1fea7f39be35 /127.0.0.1      consumer-group-assign-1
     *
     * 컨슈머 추가 등록 (eager)
     * 다 떨구고 재할당
     * [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-group-assign-1, groupId=group-assign] Revoke previously assigned partitions topic-p3-t2-1, topic-p3-t1-0, topic-p3-t2-0, topic-p3-t1-2, topic-p3-t2-2, topic-p3-t1-1
     *
     * GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                                  HOST            CLIENT-ID
     * group-assign    topic-p3-t1     0          0               0               0               consumer-group-assign-1-a4a6f5f6-5620-44ed-a6cb-ed21b1fd3e3c /127.0.0.1      consumer-group-assign-1
     * group-assign    topic-p3-t2     0          0               0               0               consumer-group-assign-1-a4a6f5f6-5620-44ed-a6cb-ed21b1fd3e3c /127.0.0.1      consumer-group-assign-1
     * group-assign    topic-p3-t2     1          1               1               0               consumer-group-assign-1-a4a6f5f6-5620-44ed-a6cb-ed21b1fd3e3c /127.0.0.1      consumer-group-assign-1
     * group-assign    topic-p3-t1     1          1               1               0               consumer-group-assign-1-a4a6f5f6-5620-44ed-a6cb-ed21b1fd3e3c /127.0.0.1      consumer-group-assign-1
     * group-assign    topic-p3-t1     2          2               2               0               consumer-group-assign-1-d62a438e-93a9-4919-9eb3-1fea7f39be35 /127.0.0.1      consumer-group-assign-1
     * group-assign    topic-p3-t2     2          2               2               0               consumer-group-assign-1-d62a438e-93a9-4919-9eb3-1fea7f39be35 /127.0.0.1      consumer-group-assign-1
     * */
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-assign");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

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
