package org.dmitrysulman.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.dmitrysulman.kafka.consumer.MyConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class MyProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyConsumer.class.getName());

    public static void main(String[] args) {
        send();
    }

    public static void send() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 100; i++) {
                String key = "key" + i;
                String value = "value" + i;
                producer.send(new ProducerRecord<>("input-test", key, value));
                LOGGER.info("Sent {} {}", key, value);
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
