package org.opensearch.dataprepper.plugins.kafka.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionType;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaRegistryType;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import software.amazon.awssdk.regions.Region;

import java.time.Duration;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerFactoryTest {
    private static final String TEST_GROUP_ID = "testGroupId";
    @Mock
    private KafkaConsumerConfig kafkaConsumerConfig;
    @Mock
    private EncryptionConfig encryptionConfig;
    @Mock
    private SchemaConfig schemaConfig;
    @Mock
    private AwsConfig awsConfig;
    @Mock
    private TopicConsumerConfig topic;
    @Mock
    private KafkaConsumer kafkaConsumer;
    @Mock
    private TopicPartition topicPartition;

    private KafkaConsumerFactory objectUnderTest;

    @BeforeEach
    void setUp() {
        lenient().when(topic.getCommitInterval()).thenReturn(Duration.ofSeconds(1));
        lenient().when(topic.getAutoOffsetReset()).thenReturn("earliest");
        lenient().when(topic.getConsumerMaxPollRecords()).thenReturn(1);
        lenient().when(topic.getGroupId()).thenReturn(TEST_GROUP_ID);
        lenient().when(topic.getMaxPollInterval()).thenReturn(Duration.ofSeconds(5));
        lenient().when(topic.getHeartBeatInterval()).thenReturn(Duration.ofSeconds(5));
        lenient().when(topic.getAutoCommit()).thenReturn(false);
        lenient().when(topic.getSessionTimeOut()).thenReturn(Duration.ofSeconds(15));
        lenient().when(kafkaConsumerConfig.getBootstrapServers()).thenReturn(
                Collections.singletonList("http://localhost:1234"));
        lenient().when(kafkaConsumerConfig.getEncryptionConfig()).thenReturn(encryptionConfig);
        lenient().when(encryptionConfig.getType()).thenReturn(EncryptionType.NONE);
    }

    @ParameterizedTest
    @MethodSource("getMessageFormat")
    void testCreateKafkaConsumerWithNullSchemaConfig(final MessageFormat messageFormat) {
        objectUnderTest = new KafkaConsumerFactory();
        when(kafkaConsumerConfig.getSchemaConfig()).thenReturn(null);
        when(topic.getSerdeFormat()).thenReturn(messageFormat);
        assertThat(objectUnderTest.createKafkaConsumer(kafkaConsumerConfig, topic), instanceOf(KafkaConsumer.class));
    }

    @Test
    void testCreateKafkaConsumerWithAwsGlueSchemaConfigType() {
        objectUnderTest = new KafkaConsumerFactory();
        when(schemaConfig.getType()).thenReturn(SchemaRegistryType.AWS_GLUE);
        when(kafkaConsumerConfig.getSchemaConfig()).thenReturn(schemaConfig);
        when(awsConfig.getRegion()).thenReturn(Region.US_EAST_1.id());
        when(kafkaConsumerConfig.getAwsConfig()).thenReturn(awsConfig);
        assertThat(objectUnderTest.createKafkaConsumer(kafkaConsumerConfig, topic), instanceOf(KafkaConsumer.class));
    }

    @Test
    void testRefreshKafkaConsumer() {
        objectUnderTest = spy(new KafkaConsumerFactory());
        final Random random = new Random();
        final Long testOffset = random.nextLong();
        final KafkaConsumer newConsumer = mock(KafkaConsumer.class);
        doReturn(newConsumer)
                .doCallRealMethod()
                .when(objectUnderTest)
                .createKafkaConsumer(eq(kafkaConsumerConfig), eq(topic));
        when(kafkaConsumer.assignment()).thenReturn(Set.of(topicPartition));
        when(kafkaConsumer.position(topicPartition)).thenReturn(testOffset);
        assertThat(objectUnderTest.refreshKafkaConsumer(kafkaConsumer, kafkaConsumerConfig, topic), is(newConsumer));
        verify(newConsumer).assign(Set.of(topicPartition));
        verify(newConsumer).seek(topicPartition, testOffset);
    }

    private static Stream<Arguments> getMessageFormat() {
        return Stream.of(
                Arguments.of(MessageFormat.PLAINTEXT),
                Arguments.of(MessageFormat.JSON),
                Arguments.of(MessageFormat.AVRO)
                );
    }
}