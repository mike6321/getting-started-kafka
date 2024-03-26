package org.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
토픽 생성
./kafka-topics --bootstrap-server localhost:9092 --create --topic multipart-topic --partitions 3
켠슈머
./kafka-console-consumer --bootstrap-server localhost:9092 --topic multipart-topic \
--property print.key=true \
--property print.value=true \
--key-deserializer "org.apache.kafka.common.serialization.IntegerDeserializer"
*/
public class ProducerASyncCustomCallBack {

    private static final Logger logger = LoggerFactory.getLogger(ProducerASyncCustomCallBack.class);

    public static void main(String[] args) {
        String topicName = "multipart-topic";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(properties);

        for (int seq = 0; seq < 20; seq++) {
            logger.info("seg : {}", seq);

            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, seq, "hello world" + seq);
            Callback callback = new CustomCallBack(seq);
            kafkaProducer.send(producerRecord, callback);
        }

        try {
            Thread.sleep(3000l);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();
    }

}
