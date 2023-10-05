package org.opensearch.dataprepper.plugins.kafka.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.dataprepper.model.plugin.PluginComponentRefresher;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSecurityConfigurer;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KafkaConsumerRefresher implements PluginComponentRefresher<KafkaConsumer, KafkaSourceConfig> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerRefresher.class);
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final KafkaConsumerFactory kafkaConsumerFactory;
    private final MessageFormat schema;
    private Properties currentConsumerProperties;
    private KafkaSourceConfig existingSourceConfig;
    private KafkaConsumer currentConsumer;

    public KafkaConsumerRefresher(final KafkaSourceConfig sourceConfig,
                                  final MessageFormat schema,
                                  final Properties consumerProperties,
                                  final KafkaConsumerFactory kafkaConsumerFactory) {
        this.schema = schema;
        this.existingSourceConfig = sourceConfig;
        this.kafkaConsumerFactory = kafkaConsumerFactory;
        currentConsumer = kafkaConsumerFactory.regenerateKafkaConsumer(sourceConfig, schema, consumerProperties);
        currentConsumerProperties = consumerProperties;
    }

    @Override
    public KafkaConsumer get() {
        readWriteLock.readLock().lock();
        try {
            return currentConsumer;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public void update(final KafkaSourceConfig kafkaSourceConfig) {
        if (plainTextAuthChanged(kafkaSourceConfig)) {
            readWriteLock.writeLock().lock();
            try {
                KafkaSecurityConfigurer.setAuthProperties(currentConsumerProperties, kafkaSourceConfig, LOG);
                currentConsumer = kafkaConsumerFactory.regenerateKafkaConsumer(
                        kafkaSourceConfig, schema, currentConsumerProperties);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }
    }

    private boolean plainTextAuthChanged(final KafkaSourceConfig newConfig) {
        final AuthConfig authConfig = newConfig.getAuthConfig();
        if (authConfig == null) {
            return false;
        }
        final AuthConfig.SaslAuthConfig saslAuthConfig = authConfig.getSaslAuthConfig();
        if (saslAuthConfig == null) {
            return false;
        }
        final PlainTextAuthConfig plainTextAuthConfig = saslAuthConfig.getPlainTextAuthConfig();
        if (plainTextAuthConfig == null) {
            return false;
        }
        final PlainTextAuthConfig existingPlainTextAuthConfig = existingSourceConfig
                .getAuthConfig().getSaslAuthConfig().getPlainTextAuthConfig();
        return !Objects.equals(plainTextAuthConfig.getUsername(), existingPlainTextAuthConfig.getUsername()) ||
                !Objects.equals(plainTextAuthConfig.getPassword(), existingPlainTextAuthConfig.getPassword());
    }
}
