package org.opensearch.dataprepper.plugins.kafka.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.dataprepper.model.plugin.PluginComponentRefresher;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.source.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaTopicConsumerMetrics;

import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KafkaConsumerRefresher implements PluginComponentRefresher<KafkaConsumer, KafkaSourceConfig> {
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final KafkaConsumerFactory kafkaConsumerFactory;
    private final TopicConsumerConfig topicConsumerConfig;
    private final KafkaTopicConsumerMetrics topicMetrics;
    private KafkaSourceConfig currentConfig;
    private KafkaConsumer currentConsumer;

    public KafkaConsumerRefresher(final KafkaConsumer initialConsumer,
                                  final KafkaSourceConfig initialConfig,
                                  final TopicConsumerConfig topicConsumerConfig,
                                  final KafkaTopicConsumerMetrics topicMetrics,
                                  final KafkaConsumerFactory kafkaConsumerFactory) {
        this.currentConsumer = initialConsumer;
        this.currentConfig = initialConfig;
        this.topicConsumerConfig = topicConsumerConfig;
        this.topicMetrics = topicMetrics;
        this.kafkaConsumerFactory = kafkaConsumerFactory;
    }

    @Override
    public Class<KafkaConsumer> getComponentClass() {
        return KafkaConsumer.class;
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
        if (basicAuthChanged(kafkaSourceConfig)) {
            readWriteLock.writeLock().lock();
            try {
                final KafkaConsumer newConsumer = kafkaConsumerFactory.refreshKafkaConsumer(
                        currentConsumer, kafkaSourceConfig, topicConsumerConfig);
                topicMetrics.deregister(currentConsumer);
                currentConsumer.close();
                topicMetrics.register(newConsumer);
                currentConsumer = newConsumer;
                currentConfig = kafkaSourceConfig;
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }
    }

    private boolean basicAuthChanged(final KafkaSourceConfig newConfig) {
        final AuthConfig currentAuthConfig = currentConfig.getAuthConfig();
        if (currentAuthConfig == null) {
            return false;
        }
        final AuthConfig.SaslAuthConfig currentSaslAuthConfig = currentAuthConfig.getSaslAuthConfig();
        if (currentSaslAuthConfig == null) {
            return false;
        }
        final PlainTextAuthConfig currentPlainTextAuthConfig = currentSaslAuthConfig.getPlainTextAuthConfig();
        if (currentPlainTextAuthConfig == null) {
            return false;
        }
        final PlainTextAuthConfig newPlainTextAuthConfig = newConfig.getAuthConfig().getSaslAuthConfig()
                .getPlainTextAuthConfig();
        return !Objects.equals(currentPlainTextAuthConfig.getUsername(), newPlainTextAuthConfig.getUsername()) ||
                !Objects.equals(currentPlainTextAuthConfig.getPassword(), newPlainTextAuthConfig.getPassword());
    }
}
