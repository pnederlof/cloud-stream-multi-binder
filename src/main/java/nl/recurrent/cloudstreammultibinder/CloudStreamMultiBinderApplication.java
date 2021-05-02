package nl.recurrent.cloudstreammultibinder;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.support.SimpleTriggerContext;

import java.util.function.Function;

@SpringBootApplication
public class CloudStreamMultiBinderApplication {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public static void main(String[] args) {
        SpringApplication.run(CloudStreamMultiBinderApplication.class, args);
    }

    // Remove in next Spring Boot Release
    @Bean
    SimpleMeterRegistry simpleMeterRegistry() {
        return new SimpleMeterRegistry();
    }

    @Bean
    public Function<KStream<String, String>, KStream<String, String>> processKafkaStreams() {
        return input -> input.peek((key, value) -> {
            logger.info("Processing with Kafka Streams Binder: {}", value);
        });
    }

//    @Bean
//    public Function<String, String> process() {
//        return input -> {
//            logger.info("Processing: {}", input);
//            return input;
//        };
//    }
}
