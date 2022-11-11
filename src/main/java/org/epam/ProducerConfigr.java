package org.epam;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerConfigr {
    private KafkaProducer<String, String> producer;
    public KafkaProducer<String, String> getProducer() {
        Properties properties = new Properties();
        try {
            properties.load(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream("app.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        producer = new KafkaProducer<String, String>(
                Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("bootstrap.servers"),
                        ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString(),
                        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.TRUE,
                        ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString(),
                        ProducerConfig.ACKS_CONFIG, "all",
                        ProducerConfig.RETRIES_CONFIG, 1,
                        ProducerConfig.BATCH_SIZE_CONFIG, 5
                ),
                new StringSerializer(),
                new StringSerializer()
        );
        producer.initTransactions();
        return producer;
    }

    public KafkaProducer<String, String> getProducer(String server) {
        producer = new KafkaProducer<String, String>(
                Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server,
                        ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString(),
                        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.TRUE,
                        ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString(),
                        ProducerConfig.ACKS_CONFIG, "all",
                        ProducerConfig.RETRIES_CONFIG, 1
                ),
                new StringSerializer(),
                new StringSerializer()
        );
        producer.initTransactions();
        return producer;
    }


}
