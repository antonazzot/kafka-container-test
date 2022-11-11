package org.epam;


import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerMain {
    private static final Properties properties = new Properties();
    private static String topic;
    private static String server;

    public static void main(String[] args) {
        try {
            properties.load(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream("app.properties"));
            topic = properties.getProperty("topic");
            server = properties.getProperty("bootstrap.servers");
        } catch (IOException e) {
            e.printStackTrace();
        }
        createTopics();
        KafkaProducer<String, String> producer = new ProducerConfigr().getProducer();
        sendMessages(producer, topic, 1000);
    }

    private static void sendMessages(KafkaProducer<String, String> producer, String topic, long record) {
        for (int i = 1; i <= 1000; i++) {
            producer.beginTransaction();
            producer.send(new ProducerRecord<String, String>(topic, Long.toString(record),
                    "Topic:  " + topic + "  Record number:  " + Long.toString(record++)));
            producer.commitTransaction();
        }
    }

    private static void createTopics() {
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, server))) {
            admin.createTopics(List.of(new NewTopic(properties.getProperty("topic"), 3, (short) 3)));
        }
    }
}

