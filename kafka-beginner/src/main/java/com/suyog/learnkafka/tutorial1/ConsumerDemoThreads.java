package com.suyog.learnkafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class ConsumerDemoThreads {
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoThreads.class.getName());

    public static void main(String[] args) {
        new ConsumerDemoThreads().run();
    }

    private ConsumerDemoThreads() {
    }

    private void run() {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-fifth-application";
        String topic = "first_topic";
        //latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating the consumer runnable thread.");
        Runnable myConsumerRunnable = new ConsumerRunnable(latch, bootstrapServers, groupId, topic);
        //start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook.");
            ((ConsumerRunnable) myConsumerRunnable).shutDown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("Application got interrupted. " + e);
            }
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted. " + e);
        } finally {
            logger.info("Application is closing.");
        }
    }

}

class ConsumerRunnable implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;

    public ConsumerRunnable(CountDownLatch latch, String bootstrapServers, String groupId, String topic) {
        this.latch = latch;
        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        consumer = new KafkaConsumer<>(properties);

        //subscribe consumer to Topic(s)
        consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        //poll for new data
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key: " + record.key() + ", Value: " + record.value());
                    logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            logger.error("Received shutdown signal! " + e);
        } finally {
            consumer.close();
            latch.countDown();
        }
    }

    public void shutDown() {
        //wakeup() is special method to interrupt consumer.poll()
        //It will throw wakeupException
        consumer.wakeup();
    }
}
