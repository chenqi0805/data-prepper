package org.opensearch.dataprepper.plugins.kafka.consumer;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSecurityConfigurer;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.util.Objects;
import java.util.Properties;

public class KafkaConsumerFactory {
    private final StringDeserializer stringDeserializer;

    public KafkaConsumerFactory(final StringDeserializer stringDeserializer) {
        this.stringDeserializer = stringDeserializer;
    }

    public KafkaConsumer regenerateKafkaConsumer(final KafkaSourceConfig sourceConfig,
                                             final MessageFormat schema,
                                             final Properties consumerProperties) {
        switch (schema) {
            case JSON:
                return new KafkaConsumer<String, JsonNode>(consumerProperties);
            case AVRO:
                return new KafkaConsumer<String, GenericRecord>(consumerProperties);
            case PLAINTEXT:
            default:
                final GlueSchemaRegistryKafkaDeserializer glueDeserializer =
                        KafkaSecurityConfigurer.getGlueSerializer(sourceConfig);
                if (Objects.nonNull(glueDeserializer)) {
                    return new KafkaConsumer(consumerProperties, stringDeserializer, glueDeserializer);
                } else {
                    return new KafkaConsumer<String, String>(consumerProperties);
                }
        }
    }
}
