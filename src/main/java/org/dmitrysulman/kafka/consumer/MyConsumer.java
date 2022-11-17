package org.dmitrysulman.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class MyConsumer implements Runnable {
    private final KafkaConsumer<String, String> consumer;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final CountDownLatch latch = new CountDownLatch(1);
    private static final Logger LOGGER = LoggerFactory.getLogger(MyConsumer.class.getName());

    public static void main(String[] args) {
        MyConsumer myConsumer = new MyConsumer();
        Thread thread = new Thread(myConsumer);
        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                try {
                    myConsumer.shutdown();
                } catch (Throwable e) {
                    System.exit(1);
                }
            }
        });
        thread.start();
    }

    public MyConsumer() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test" + System.currentTimeMillis());
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
    }

    public void shutdown() throws InterruptedException {
        LOGGER.info("Stopping...");
        closed.set(true);
        consumer.wakeup();
        latch.await();
    }

    public void run() {
        try {
            consumer.subscribe(List.of("input-test"));
            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records)
                    System.out.printf("offset = %d, key = %s, value = %s, ts = %tT, topic = %s, partition = %d%n", record.offset(), record.key(), record.value(), record.timestamp(), record.topic(), record.partition());
            }
        } catch (WakeupException e) {
            if (!closed.get()) {
                throw e;
            }
        } finally {
            consumer.close();
            LOGGER.info("Stopped.");
            latch.countDown();
        }
    }
}