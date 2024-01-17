package org.opensearch.dataprepper.plugins.kafka.configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConsumerSchemaTypeFactory.DEFAULT_SCHEMA_TYPE;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerSchemaTypeFactoryTest {
    @Mock
    private KafkaConsumerConfig kafkaConsumerConfig;
    @Mock
    private SchemaConfig schemaConfig;
    @Mock
    private TopicConsumerConfig topic;
    private final KafkaConsumerSchemaTypeFactory objectUnderTest = new KafkaConsumerSchemaTypeFactory();

    @ParameterizedTest
    @MethodSource("getMessageFormat")
    void testDeriveSchemaTypeWithNullSchemaConfig(final MessageFormat messageFormat) {
        when(kafkaConsumerConfig.getSchemaConfig()).thenReturn(null);
        when(topic.getSerdeFormat()).thenReturn(messageFormat);
        assertThat(objectUnderTest.deriveSchemaType(kafkaConsumerConfig, topic), equalTo(messageFormat));
    }

    @Test
    void testDeriveSchemaTypeWithAwsGlueSchemaConfigType() {
        when(schemaConfig.getType()).thenReturn(SchemaRegistryType.AWS_GLUE);
        when(kafkaConsumerConfig.getSchemaConfig()).thenReturn(schemaConfig);
        assertThat(objectUnderTest.deriveSchemaType(kafkaConsumerConfig, topic), equalTo(DEFAULT_SCHEMA_TYPE));
    }

    private static Stream<Arguments> getMessageFormat() {
        return Stream.of(
                Arguments.of(MessageFormat.PLAINTEXT),
                Arguments.of(MessageFormat.JSON),
                Arguments.of(MessageFormat.AVRO)
        );
    }
}