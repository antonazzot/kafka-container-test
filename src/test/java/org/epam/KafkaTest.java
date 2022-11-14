package org.epam;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class KafkaTest {
    private final ProducerConfigr producerConfigr = new ProducerConfigr();
    private final ConsumerConfigr consumerConfigr = new ConsumerConfigr();
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;
    private final String TOPIC_NAME = "messages-" + UUID.randomUUID();
    private final DockerImageName KAFKA_TEST_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:6.2.1");
    private final DockerImageName ZOOKEEPER_TEST_IMAGE = DockerImageName.parse("confluentinc/cp-zookeeper:4.0.0");

    @Test
    void producer_consumer_config_test_case() throws Exception {
        try (KafkaContainer kafka = new KafkaContainer(KAFKA_TEST_IMAGE)) {
            kafka.start();
            testKafkaFunctionality(kafka.getBootstrapServers());
        }
    }

    @Test
    void producer_consumer_config_test_case_with_specific_image() throws Exception {
        try (
                // constructorWithVersion {
                KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
                // }
        ) {
            kafka.start();
            testKafkaFunctionality(
                    // getBootstrapServers {
                    kafka.getBootstrapServers()
                    // }
            );
        }
    }

    @Test
    void producer_consumer_test_case_with_version() throws Exception {
        try (KafkaContainer kafka = new KafkaContainer("6.2.1")) {
            kafka.start();
            testKafkaFunctionality(kafka.getBootstrapServers());
        }
    }

    @Test
    void kafka_test_with_external_Zookeeper_with_Network() throws Exception {
        try (
                Network network = Network.newNetwork();
                KafkaContainer kafka = new KafkaContainer(KAFKA_TEST_IMAGE)
                        .withNetwork(network)
                        .withExternalZookeeper("zookeeper:2181");
                GenericContainer<?> zookeeper = new GenericContainer<>(ZOOKEEPER_TEST_IMAGE)
                        .withNetwork(network)
                        .withNetworkAliases("zookeeper")
                        .withEnv("ZOOKEEPER_CLIENT_PORT", "2181");
                GenericContainer<?> application = new GenericContainer<>(DockerImageName.parse("alpine"))
                        .withNetwork(network)
                        .withNetworkAliases("dummy")
                        .withCommand("sleep 10000")
        ) {
            zookeeper.start();
            kafka.start();
            application.start();

            testKafkaFunctionality(kafka.getBootstrapServers());
        }
    }

    @Test
    void kafka_test_with_HostExposedPort() throws Exception {
        org.testcontainers.Testcontainers.exposeHostPorts(12345);
        try (KafkaContainer kafka = new KafkaContainer(KAFKA_TEST_IMAGE)) {
            kafka.start();
            testKafkaFunctionality(kafka.getBootstrapServers());
        }
    }
    @Test
    void kafka_different_cases() throws Exception {
        try (KafkaContainer kafka = new KafkaContainer(KAFKA_TEST_IMAGE)) {
            kafka.start();
            testKafkaFunctionality(kafka.getBootstrapServers(), 2, 4);
        }
    }

    private void testKafkaFunctionality(String bootstrapServers) throws Exception {
        testKafkaFunctionality(bootstrapServers, 1, 1);
    }

    private void testKafkaFunctionality(String bootstrapServers, int partitions, int rf) throws Exception {
        try {
            producer = producerConfigr.getProducer(bootstrapServers);
            consumer = consumerConfigr.getConsumer(bootstrapServers);

            createTopics(1, 1, bootstrapServers, TOPIC_NAME);

            consumer.subscribe(Collections.singletonList(TOPIC_NAME));

            producer.beginTransaction();
            producer.send(new ProducerRecord<>(TOPIC_NAME, "testkey", "testvalue")).get();
            producer.commitTransaction();

            Unreliables.retryUntilTrue(
                    50,
                    TimeUnit.SECONDS,
                    () -> {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                        if (records.isEmpty()) {
                            return false;
                        }
                        assertThat(records)
                                .hasSize(1)
                                .extracting(ConsumerRecord::topic, ConsumerRecord::key, ConsumerRecord::value)
                                .containsExactly(tuple(TOPIC_NAME, "testkey", "testvalue"));

                        return true;
                    }
            );

            consumer.unsubscribe();
        } finally {
            producer.close();
            consumer.close();
        }
    }

    private static void createTopics(int part, int rf, String broker, String... topics) throws Exception {
        var newTopics =
                Arrays.stream(topics)
                        .map(topic -> new NewTopic(topic, part, (short) rf))
                        .collect(Collectors.toList());
        try (
                var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, broker))) {
            admin.createTopics(newTopics).all().get(30, TimeUnit.SECONDS);
        }
    }
}
