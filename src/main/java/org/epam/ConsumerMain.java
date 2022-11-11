package org.epam;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerMain {
    private static final Properties properties = new Properties();
    private static String topic;

    public static void main(String[] args) {
        try {
            properties.load(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream("app.properties"));
            topic = properties.getProperty("topic");
        } catch (IOException e) {
            e.printStackTrace();
        }
        KafkaConsumer<String, String> consumer = new ConsumerConfigr().getConsumer();
        consumer.subscribe(List.of(topic));
        processRecords(consumer);
    }

    private static void processRecords(KafkaConsumer<String, String> consumer) {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            long lastOffset = 0;
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("\n\roffset = %d, key = %s, value = %s", record.offset(), record.key(),
                        record.value());
                lastOffset = record.offset();
            }
            System.out.println("lastOffset read: " + lastOffset);
            try {
                process();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void process() throws InterruptedException {
        Thread.sleep(2000);
        System.out.println(".......");
    }
}
