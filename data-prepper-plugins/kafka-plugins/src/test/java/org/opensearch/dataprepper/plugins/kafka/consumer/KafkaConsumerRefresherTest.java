package org.opensearch.dataprepper.plugins.kafka.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.source.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaTopicConsumerMetrics;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerRefresherTest {
    private static final String TEST_USERNAME = "test_user";
    private static final String TEST_PASSWORD = "test_password";

    @Mock
    private KafkaConsumer kafkaConsumer;
    @Mock
    private KafkaSourceConfig kafkaSourceConfig;
    @Mock
    private TopicConsumerConfig topicConsumerConfig;
    @Mock
    private AuthConfig authConfig;
    @Mock
    private AuthConfig.SaslAuthConfig saslAuthConfig;
    @Mock
    private PlainTextAuthConfig plainTextAuthConfig;
    @Mock
    private KafkaTopicConsumerMetrics kafkaTopicConsumerMetrics;
    @Mock
    private KafkaConsumerFactory kafkaConsumerFactory;

    private KafkaConsumerRefresher objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = new KafkaConsumerRefresher(
                kafkaConsumer,
                kafkaSourceConfig,
                topicConsumerConfig,
                kafkaTopicConsumerMetrics,
                kafkaConsumerFactory
        );
    }

    @Test
    void testGet() {
        assertThat(objectUnderTest.get(), equalTo(kafkaConsumer));
    }

    @Test
    void testUpdateFromNullAuthConfig() {
        when(kafkaSourceConfig.getAuthConfig()).thenReturn(null);
        final KafkaSourceConfig newConfig = mock(KafkaSourceConfig.class);
        objectUnderTest.update(newConfig);
        assertThat(objectUnderTest.get(), equalTo(kafkaConsumer));
        verifyNoInteractions(kafkaConsumerFactory);
    }

    @Test
    void testUpdateFromNullSaslAuthConfig() {
        when(kafkaSourceConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(null);
        final KafkaSourceConfig newConfig = mock(KafkaSourceConfig.class);
        objectUnderTest.update(newConfig);
        assertThat(objectUnderTest.get(), equalTo(kafkaConsumer));
        verifyNoInteractions(kafkaConsumerFactory);
    }

    @Test
    void testUpdateFromNullPlainTextAuthConfig() {
        when(kafkaSourceConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        when(saslAuthConfig.getPlainTextAuthConfig()).thenReturn(null);
        final KafkaSourceConfig newConfig = mock(KafkaSourceConfig.class);
        objectUnderTest.update(newConfig);
        assertThat(objectUnderTest.get(), equalTo(kafkaConsumer));
        verifyNoInteractions(kafkaConsumerFactory);
    }

    @Test
    void testGetAfterUpdateWithBasicAuthUnchanged() {
        when(kafkaSourceConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        when(saslAuthConfig.getPlainTextAuthConfig()).thenReturn(plainTextAuthConfig);
        when(plainTextAuthConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(plainTextAuthConfig.getPassword()).thenReturn(TEST_PASSWORD);
        final KafkaSourceConfig newConfig = mock(KafkaSourceConfig.class);
        final AuthConfig newAuthConfig = mock(AuthConfig.class);
        when(newConfig.getAuthConfig()).thenReturn(newAuthConfig);
        final AuthConfig.SaslAuthConfig newSaslAuthConfig = mock(AuthConfig.SaslAuthConfig.class);
        when(newAuthConfig.getSaslAuthConfig()).thenReturn(newSaslAuthConfig);
        final PlainTextAuthConfig newPlainTextAuthConfig = mock(PlainTextAuthConfig.class);
        when(newSaslAuthConfig.getPlainTextAuthConfig()).thenReturn(newPlainTextAuthConfig);
        when(newPlainTextAuthConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(newPlainTextAuthConfig.getPassword()).thenReturn(TEST_PASSWORD);
        objectUnderTest.update(newConfig);
        assertThat(objectUnderTest.get(), equalTo(kafkaConsumer));
        verifyNoInteractions(kafkaConsumerFactory);
    }

    @Test
    void testGetAfterUpdateWithUsernameChanged() {
        when(kafkaSourceConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        when(saslAuthConfig.getPlainTextAuthConfig()).thenReturn(plainTextAuthConfig);
        when(plainTextAuthConfig.getUsername()).thenReturn(TEST_USERNAME);
        final KafkaSourceConfig newConfig = mock(KafkaSourceConfig.class);
        final AuthConfig newAuthConfig = mock(AuthConfig.class);
        when(newConfig.getAuthConfig()).thenReturn(newAuthConfig);
        final AuthConfig.SaslAuthConfig newSaslAuthConfig = mock(AuthConfig.SaslAuthConfig.class);
        when(newAuthConfig.getSaslAuthConfig()).thenReturn(newSaslAuthConfig);
        final PlainTextAuthConfig newPlainTextAuthConfig = mock(PlainTextAuthConfig.class);
        when(newSaslAuthConfig.getPlainTextAuthConfig()).thenReturn(newPlainTextAuthConfig);
        when(newPlainTextAuthConfig.getUsername()).thenReturn(TEST_USERNAME + "_changed");
        final KafkaConsumer newConsumer = mock(KafkaConsumer.class);
        when(kafkaConsumerFactory.refreshKafkaConsumer(eq(kafkaConsumer), eq(newConfig), eq(topicConsumerConfig)))
                .thenReturn(newConsumer);
        objectUnderTest.update(newConfig);
        assertThat(objectUnderTest.get(), equalTo(newConsumer));
        verify(kafkaTopicConsumerMetrics).deregister(kafkaConsumer);
        verify(kafkaConsumer).close();
        verify(kafkaTopicConsumerMetrics).register(newConsumer);
    }

    @Test
    void testGetAfterUpdateWithPasswordChanged() {
        when(kafkaSourceConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        when(saslAuthConfig.getPlainTextAuthConfig()).thenReturn(plainTextAuthConfig);
        when(plainTextAuthConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(plainTextAuthConfig.getPassword()).thenReturn(TEST_PASSWORD);
        final KafkaSourceConfig newConfig = mock(KafkaSourceConfig.class);
        final AuthConfig newAuthConfig = mock(AuthConfig.class);
        when(newConfig.getAuthConfig()).thenReturn(newAuthConfig);
        final AuthConfig.SaslAuthConfig newSaslAuthConfig = mock(AuthConfig.SaslAuthConfig.class);
        when(newAuthConfig.getSaslAuthConfig()).thenReturn(newSaslAuthConfig);
        final PlainTextAuthConfig newPlainTextAuthConfig = mock(PlainTextAuthConfig.class);
        when(newSaslAuthConfig.getPlainTextAuthConfig()).thenReturn(newPlainTextAuthConfig);
        when(newPlainTextAuthConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(newPlainTextAuthConfig.getPassword()).thenReturn(TEST_PASSWORD + "_changed");
        final KafkaConsumer newConsumer = mock(KafkaConsumer.class);
        when(kafkaConsumerFactory.refreshKafkaConsumer(eq(kafkaConsumer), eq(newConfig), eq(topicConsumerConfig)))
                .thenReturn(newConsumer);
        objectUnderTest.update(newConfig);
        assertThat(objectUnderTest.get(), equalTo(newConsumer));
        verify(kafkaTopicConsumerMetrics).deregister(kafkaConsumer);
        verify(kafkaConsumer).close();
        verify(kafkaTopicConsumerMetrics).register(newConsumer);
    }
}