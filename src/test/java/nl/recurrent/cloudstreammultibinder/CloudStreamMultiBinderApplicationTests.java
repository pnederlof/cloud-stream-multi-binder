package nl.recurrent.cloudstreammultibinder;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EmbeddedKafka(
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        partitions = 1
)
class CloudStreamMultiBinderApplicationTests {

    @Autowired
    EmbeddedKafkaBroker embeddedKafka;

    private Consumer<String, String> kafkaConsumer;
    private Consumer<String, String> kafkaStreamsConsumer;
    private Consumer<String, String> testSetupConsumer;

    private Producer<String, String> producer;

    @BeforeEach()
    void setUp() {
        producer = createProducer();

        kafkaConsumer = createConsumer("group-kafka-binder");
        kafkaConsumer.subscribe(List.of("processKafka-out-0"));
        kafkaConsumer.poll(Duration.ofMillis(200L));

        kafkaStreamsConsumer = createConsumer("group-kafka-streams-binder");
        kafkaStreamsConsumer.subscribe(List.of("processKafkaStreams-out-0"));
        kafkaStreamsConsumer.poll(Duration.ofMillis(200L));

        testSetupConsumer = createConsumer("group-test-setup");
        testSetupConsumer.subscribe(List.of("processKafkaStreams-in-0"));
        testSetupConsumer.poll(Duration.ofMillis(200L));
    }


    @Test
    void testSetupWorks() {
        producer.send(new ProducerRecord<>("processKafkaStreams-in-0", "key", "value"));

        ConsumerRecords<String, String> consumerRecords = testSetupConsumer.poll(Duration.ofSeconds(5L));

        assertThat(consumerRecords).hasSize(1);
        assertThat(consumerRecords.records("processKafkaStreams-in-0").iterator().next().value()).isEqualTo("value");
    }

    @Test
    void kafkaStreamsBinder_processesMessage() {
        producer.send(new ProducerRecord<>("processKafkaStreams-in-0", "key", "valueKafkaStreams"));

        ConsumerRecords<String, String> consumerRecords = kafkaStreamsConsumer.poll(Duration.ofSeconds(5L));

        assertThat(consumerRecords).hasSize(1);
        assertThat(consumerRecords.records("processKafkaStreams-out-0").iterator().next().value()).isEqualTo("valueKafkaStreams");
    }

    @Test
    void kafkaBinder_processesMessage() {
        producer.send(new ProducerRecord<>("processKafka-in-0", "key", "valueKafka"));

        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(5L));

        assertThat(consumerRecords).hasSize(1);
        assertThat(consumerRecords.records("processKafka-out-0").iterator().next().value()).isEqualTo("valueKafka");
    }

    @AfterEach
    void tearDown() {
        producer.close();
        kafkaConsumer.close();
        kafkaStreamsConsumer.close();
        testSetupConsumer.close();
    }

    private Producer<String, String> createProducer() {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return ((ProducerFactory<String, String>) new DefaultKafkaProducerFactory<String, String>(producerProps)).createProducer();
    }

    private Consumer<String, String> createConsumer(String groupId) {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(groupId, "true", embeddedKafka);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<String, String>(consumerProps).createConsumer();
    }

}
