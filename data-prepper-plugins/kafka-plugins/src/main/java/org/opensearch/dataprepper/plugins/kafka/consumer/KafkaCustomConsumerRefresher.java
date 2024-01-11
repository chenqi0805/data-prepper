/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginComponentRefresher;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.source.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaTopicConsumerMetrics;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KafkaCustomConsumerRefresher implements PluginComponentRefresher<KafkaCustomConsumer, KafkaSourceConfig> {
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private KafkaSourceConfig currentConfig;
    private KafkaCustomConsumer currentConsumer;
    private final KafkaTopicConsumerMetrics topicMetrics;
    private final KafkaSourceCustomConsumerFactory kafkaSourceCustomConsumerFactory;
    private final AtomicBoolean shutdownInProgress;
    private final AcknowledgementSetManager acknowledgementSetManager;

    public KafkaCustomConsumerRefresher(final KafkaCustomConsumer initialConsumer,
                                        final KafkaSourceConfig initialConfig,
                                        final KafkaTopicConsumerMetrics topicMetrics,
                                        final AtomicBoolean shutdownInProgress,
                                        final AcknowledgementSetManager acknowledgementSetManager,
                                        final KafkaSourceCustomConsumerFactory kafkaSourceCustomConsumerFactory) {
        this.currentConsumer = initialConsumer;
        this.currentConfig = initialConfig;
        this.topicMetrics = topicMetrics;
        this.shutdownInProgress = shutdownInProgress;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.kafkaSourceCustomConsumerFactory = kafkaSourceCustomConsumerFactory;
    }

    @Override
    public Class<KafkaCustomConsumer> getComponentClass() {
        return KafkaCustomConsumer.class;
    }

    @Override
    public KafkaCustomConsumer get() {
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
                currentConsumer.closeConsumer();
                topicMetrics.deregister(currentConsumer.getConsumer());
                currentConsumer = kafkaSourceCustomConsumerFactory.refreshConsumerForTopic(
                        currentConsumer, kafkaSourceConfig, topicMetrics,
                        shutdownInProgress, acknowledgementSetManager);
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
