package org.opensearch.dataprepper.plugins.kafka.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSourceSecurityConfigurer;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.lang.management.ManagementFactory;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerRefresherTest {
    private static final String TEST_USERNAME = "test_username";
    private static final String TEST_PASSWORD = "test_password";
    @Mock
    private KafkaSourceConfig kafkaSourceConfig;

    @Mock
    private AuthConfig authConfig;

    @Mock
    private AuthConfig.SaslAuthConfig saslAuthConfig;

    @Mock
    private PlainTextAuthConfig plainTextAuthConfig;

    @Mock
    private MessageFormat schema;

    @Mock
    private Properties consumerProperties;

    @Mock
    private KafkaConsumerFactory kafkaConsumerFactory;

    @Mock
    private KafkaConsumer kafkaConsumer;

    private KafkaConsumerRefresher objectUnderTest;

    @BeforeEach
    void setup() {
        when(kafkaConsumerFactory.regenerateKafkaConsumer(eq(kafkaSourceConfig), eq(schema), eq(consumerProperties)))
                .thenReturn(kafkaConsumer);
        objectUnderTest = new KafkaConsumerRefresher(
                kafkaSourceConfig, schema, consumerProperties, kafkaConsumerFactory);
    }

    @Test
    void testGet() {
        assertThat(objectUnderTest.get(), is(kafkaConsumer));
    }

    @Test
    void testGetAfterUpdateWithNullAuthConfig() {
        final KafkaSourceConfig newConfig = mock(KafkaSourceConfig.class);
        objectUnderTest.update(newConfig);
        assertThat(objectUnderTest.get(), is(kafkaConsumer));
    }

    @Test
    void testGetAfterUpdateWithNullSaslAuthConfig() {
        final KafkaSourceConfig newConfig = mock(KafkaSourceConfig.class);
        final AuthConfig newAuthConfig = mock(AuthConfig.class);
        when(newConfig.getAuthConfig()).thenReturn(newAuthConfig);
        objectUnderTest.update(newConfig);
        assertThat(objectUnderTest.get(), is(kafkaConsumer));
    }

    @Test
    void testGetAfterUpdateWithNullPlainTextAuthConfig() {
        final KafkaSourceConfig newConfig = mock(KafkaSourceConfig.class);
        final AuthConfig newAuthConfig = mock(AuthConfig.class);
        final AuthConfig.SaslAuthConfig newSaslAuthConfig = mock(AuthConfig.SaslAuthConfig.class);
        when(newConfig.getAuthConfig()).thenReturn(newAuthConfig);
        when(newAuthConfig.getSaslAuthConfig()).thenReturn(newSaslAuthConfig);
        objectUnderTest.update(newConfig);
        assertThat(objectUnderTest.get(), is(kafkaConsumer));
    }

    @Test
    void testGetAfterUpdateWithPlainTextAuthUnchanged() {
        when(kafkaSourceConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        when(saslAuthConfig.getPlainTextAuthConfig()).thenReturn(plainTextAuthConfig);
        when(plainTextAuthConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(plainTextAuthConfig.getPassword()).thenReturn(TEST_PASSWORD);
        final KafkaSourceConfig newConfig = mock(KafkaSourceConfig.class);
        final AuthConfig newAuthConfig = mock(AuthConfig.class);
        final AuthConfig.SaslAuthConfig newSaslAuthConfig = mock(AuthConfig.SaslAuthConfig.class);
        final PlainTextAuthConfig newPlainTextAuthConfig = mock(PlainTextAuthConfig.class);
        when(newConfig.getAuthConfig()).thenReturn(newAuthConfig);
        when(newAuthConfig.getSaslAuthConfig()).thenReturn(newSaslAuthConfig);
        when(newSaslAuthConfig.getPlainTextAuthConfig()).thenReturn(newPlainTextAuthConfig);
        when(newPlainTextAuthConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(newPlainTextAuthConfig.getPassword()).thenReturn(TEST_PASSWORD);
        objectUnderTest.update(newConfig);
        assertThat(objectUnderTest.get(), is(kafkaConsumer));
    }

    @Test
    void testGetAfterUpdateWithPlainTextAuthUsernameChanged() {
        when(kafkaSourceConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        when(saslAuthConfig.getPlainTextAuthConfig()).thenReturn(plainTextAuthConfig);
        when(plainTextAuthConfig.getUsername()).thenReturn(TEST_USERNAME);
        final KafkaSourceConfig newConfig = mock(KafkaSourceConfig.class);
        final AuthConfig newAuthConfig = mock(AuthConfig.class);
        final AuthConfig.SaslAuthConfig newSaslAuthConfig = mock(AuthConfig.SaslAuthConfig.class);
        final PlainTextAuthConfig newPlainTextAuthConfig = mock(PlainTextAuthConfig.class);
        when(newConfig.getAuthConfig()).thenReturn(newAuthConfig);
        when(newAuthConfig.getSaslAuthConfig()).thenReturn(newSaslAuthConfig);
        when(newSaslAuthConfig.getPlainTextAuthConfig()).thenReturn(newPlainTextAuthConfig);
        when(newPlainTextAuthConfig.getUsername()).thenReturn(TEST_USERNAME + "_changed");
        final KafkaConsumer newConsumer = mock(KafkaConsumer.class);
        when(kafkaConsumerFactory.regenerateKafkaConsumer(eq(newConfig), eq(schema), eq(consumerProperties)))
                .thenReturn(newConsumer);
        try (final MockedStatic<KafkaSourceSecurityConfigurer> kafkaSourceSecurityConfigurerMockedStatic =
                     mockStatic(KafkaSourceSecurityConfigurer.class)) {
            kafkaSourceSecurityConfigurerMockedStatic.when(() ->
                    KafkaSourceSecurityConfigurer.setAuthProperties(eq(consumerProperties), eq(newConfig), any())
            ).thenAnswer(invocation -> null);
            objectUnderTest.update(newConfig);
        }
        assertThat(objectUnderTest.get(), is(newConsumer));
    }

    @Test
    void testGetAfterUpdateWithPlainTextAuthPasswordChanged() {
        when(kafkaSourceConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        when(saslAuthConfig.getPlainTextAuthConfig()).thenReturn(plainTextAuthConfig);
        when(plainTextAuthConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(plainTextAuthConfig.getPassword()).thenReturn(TEST_PASSWORD);
        final KafkaSourceConfig newConfig = mock(KafkaSourceConfig.class);
        final AuthConfig newAuthConfig = mock(AuthConfig.class);
        final AuthConfig.SaslAuthConfig newSaslAuthConfig = mock(AuthConfig.SaslAuthConfig.class);
        final PlainTextAuthConfig newPlainTextAuthConfig = mock(PlainTextAuthConfig.class);
        when(newConfig.getAuthConfig()).thenReturn(newAuthConfig);
        when(newAuthConfig.getSaslAuthConfig()).thenReturn(newSaslAuthConfig);
        when(newSaslAuthConfig.getPlainTextAuthConfig()).thenReturn(newPlainTextAuthConfig);
        when(newPlainTextAuthConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(newPlainTextAuthConfig.getPassword()).thenReturn(TEST_PASSWORD + "_changed");
        final KafkaConsumer newConsumer = mock(KafkaConsumer.class);
        when(kafkaConsumerFactory.regenerateKafkaConsumer(eq(newConfig), eq(schema), eq(consumerProperties)))
                .thenReturn(newConsumer);
        try (final MockedStatic<KafkaSourceSecurityConfigurer> kafkaSourceSecurityConfigurerMockedStatic =
                     mockStatic(KafkaSourceSecurityConfigurer.class)) {
            kafkaSourceSecurityConfigurerMockedStatic.when(() ->
                    KafkaSourceSecurityConfigurer.setAuthProperties(eq(consumerProperties), eq(newConfig), any())
            ).thenAnswer(invocation -> null);
            objectUnderTest.update(newConfig);
        }
        assertThat(objectUnderTest.get(), is(newConsumer));
    }
}