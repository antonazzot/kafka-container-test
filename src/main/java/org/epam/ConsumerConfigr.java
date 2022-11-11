package org.epam;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerConfigr {
    private KafkaConsumer<String, String> consumer;

    public KafkaConsumer<String, String> getConsumer() {
        Properties properties = new Properties();
        try {
            properties.load(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream("app.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        consumer = new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("bootstrap.servers"),
                        ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.TRUE,
                        ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 101,
                        ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 120
                ),
                new StringDeserializer(),
                new StringDeserializer()
        );
        return consumer;
    }

    public KafkaConsumer<String, String> getConsumer(String server) {

        consumer = new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server,
                        ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.TRUE,
                        ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 101,
                        ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 120),
                new StringDeserializer(),
                new StringDeserializer()
        );
        return consumer;
    }

}
