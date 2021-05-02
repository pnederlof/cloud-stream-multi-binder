package nl.recurrent.cloudstreammultibinder;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EmbeddedKafka(
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        partitions = 1
)
class CloudStreamMultiBinderApplicationTests {

    @Autowired
    EmbeddedKafkaBroker embeddedKafka;

    private Consumer<String, String> consumer;

    private Producer<String, String> producer;

    @BeforeEach
    void setUp() {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put("value.serializer", StringSerializer.class);
        producerProps.put("key.serializer", StringSerializer.class);

        ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProps);
        producer = producerFactory.createProducer();

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group-kafka", "true", embeddedKafka);
        consumerProps.put("value.deserializer", StringDeserializer.class);
        consumerProps.put("key.deserializer", StringDeserializer.class);
        consumerProps.put("auto.offset.reset", "earliest");
        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);

        consumer = consumerFactory.createConsumer();
        consumer.subscribe(List.of("processKafkaStreams-out-0"));
        
        consumer.poll(Duration.ofMillis(200L));
    }

    @Test
    void kafkaStreamsBinder_processesMessage() {
        producer.send(new ProducerRecord<>("processKafkaStreams-in-0", "key", "value"));
        producer.flush();

        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(5L));

        assertThat(consumerRecords).hasSize(1);
        assertThat(consumerRecords.records("processKafkaStreams-out-0").iterator().next().value()).isEqualTo("value");
    }

    @AfterEach
    void tearDown() {
        producer.close();
        consumer.close();
    }
}
