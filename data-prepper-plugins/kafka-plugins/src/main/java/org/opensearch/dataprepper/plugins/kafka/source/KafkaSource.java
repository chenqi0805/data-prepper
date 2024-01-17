/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import kafka.common.BrokerEndPointNotAvailableException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConsumerSchemaTypeFactory;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.consumer.KafkaConsumerFactory;
import org.opensearch.dataprepper.plugins.kafka.consumer.KafkaConsumerRefresher;
import org.opensearch.dataprepper.plugins.kafka.consumer.KafkaCustomConsumer;
import org.opensearch.dataprepper.plugins.kafka.consumer.PauseConsumePredicate;
import org.opensearch.dataprepper.plugins.kafka.extension.KafkaClusterConfigSupplier;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSecurityConfigurer;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaTopicConsumerMetrics;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * The starting point of the Kafka-source plugin and the Kafka consumer
 * properties and kafka multithreaded consumers are being handled here.
 */

@SuppressWarnings("deprecation")
@DataPrepperPlugin(name = "kafka", pluginType = Source.class, pluginConfigurationType = KafkaSourceConfig.class)
public class KafkaSource implements Source<Record<Event>> {
    private static final String NO_RESOLVABLE_URLS_ERROR_MESSAGE = "No resolvable bootstrap urls given in bootstrap.servers";
    private static final long RETRY_SLEEP_INTERVAL = 30000;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);
    private final KafkaConsumerSchemaTypeFactory kafkaConsumerSchemaTypeFactory = new KafkaConsumerSchemaTypeFactory();
    private final KafkaSourceConfig sourceConfig;
    private final AtomicBoolean shutdownInProgress;
    private ExecutorService executorService;
    private final PluginMetrics pluginMetrics;
    private final PluginConfigObservable pluginConfigObservable;
    private KafkaCustomConsumer consumer;
    private final String pipelineName;
    private String consumerGroupID;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private static CachedSchemaRegistryClient schemaRegistryClient;
    private GlueSchemaRegistryKafkaDeserializer glueDeserializer;
    private StringDeserializer stringDeserializer;
    private final List<ExecutorService> allTopicExecutorServices;
    private final List<KafkaCustomConsumer> allTopicConsumers;

    @DataPrepperPluginConstructor
    public KafkaSource(final KafkaSourceConfig sourceConfig,
                       final PluginMetrics pluginMetrics,
                       final AcknowledgementSetManager acknowledgementSetManager,
                       final PipelineDescription pipelineDescription,
                       final KafkaClusterConfigSupplier kafkaClusterConfigSupplier,
                       final PluginConfigObservable pluginConfigObservable) {
        this.sourceConfig = sourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.pluginConfigObservable = pluginConfigObservable;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.pipelineName = pipelineDescription.getPipelineName();
        this.stringDeserializer = new StringDeserializer();
        this.shutdownInProgress = new AtomicBoolean(false);
        this.allTopicExecutorServices = new ArrayList<>();
        this.allTopicConsumers = new ArrayList<>();
        this.updateConfig(kafkaClusterConfigSupplier);
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        Properties authProperties = new Properties();
        KafkaSecurityConfigurer.setAuthProperties(authProperties, sourceConfig, LOG);
        sourceConfig.getTopics().forEach(topic -> {
            consumerGroupID = topic.getGroupId();
            KafkaTopicConsumerMetrics topicMetrics = new KafkaTopicConsumerMetrics(topic.getName(), pluginMetrics, true);
            try {
                int numWorkers = topic.getWorkers();
                executorService = Executors.newFixedThreadPool(numWorkers);
                allTopicExecutorServices.add(executorService);

                IntStream.range(0, numWorkers).forEach(index -> {
                    final MessageFormat schema = kafkaConsumerSchemaTypeFactory.deriveSchemaType(sourceConfig, topic);
                    final KafkaConsumerFactory kafkaConsumerFactory = new KafkaConsumerFactory(schema);
                    final KafkaConsumer kafkaConsumer = kafkaConsumerFactory.createKafkaConsumer(sourceConfig, topic);
                    final KafkaConsumerRefresher kafkaConsumerRefresher = new KafkaConsumerRefresher(
                            kafkaConsumer, sourceConfig, topic, topicMetrics, kafkaConsumerFactory);
                    pluginConfigObservable.addPluginConfigObserver(
                            newConfig -> kafkaConsumerRefresher.update((KafkaSourceConfig) newConfig));
                    consumer = new KafkaCustomConsumer(kafkaConsumerRefresher::get, shutdownInProgress, buffer, sourceConfig, topic, schema.name(),
                            acknowledgementSetManager, null, topicMetrics, PauseConsumePredicate.noPause());
                    allTopicConsumers.add(consumer);

                    executorService.submit(consumer);
                });
            } catch (Exception e) {
                if (e instanceof BrokerNotAvailableException ||
                        e instanceof BrokerEndPointNotAvailableException || e instanceof TimeoutException) {
                    LOG.error("The kafka broker is not available...");
                } else {
                    LOG.error("Failed to setup the Kafka Source Plugin.", e);
                }
                throw new RuntimeException(e);
            }
            LOG.info("Started Kafka source for topic " + topic.getName());
        });
    }

    @Override
    public void stop() {
        shutdownInProgress.set(true);
        final long shutdownWaitTime = calculateLongestThreadWaitingTime();

        LOG.info("Shutting down {} Executor services", allTopicExecutorServices.size());
        allTopicExecutorServices.forEach(executor -> stopExecutor(executor, shutdownWaitTime));

        LOG.info("Closing {} consumers", allTopicConsumers.size());
        allTopicConsumers.forEach(consumer -> consumer.closeConsumer());

        LOG.info("Kafka source shutdown successfully...");
    }

    public void stopExecutor(final ExecutorService executorService, final long shutdownWaitTime) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(shutdownWaitTime, TimeUnit.SECONDS)) {
                LOG.info("Consumer threads are waiting for shutting down...");
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            if (e.getCause() instanceof InterruptedException) {
                LOG.error("Interrupted during consumer shutdown, exiting uncleanly...", e);
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    KafkaCustomConsumer getConsumer() {
        return consumer;
    }

    KafkaConsumerSchemaTypeFactory getKafkaConsumerSchemaTypeFactory() {
        return kafkaConsumerSchemaTypeFactory;
    }

    private long calculateLongestThreadWaitingTime() {
        List<? extends TopicConsumerConfig> topicsList = sourceConfig.getTopics();
        return topicsList.stream().
                map(
                        topics -> topics.getThreadWaitingTime().toSeconds()
                ).
                max(Comparator.comparingLong(time -> time)).
                orElse(1L);
    }

    protected void sleep(final long millis) throws InterruptedException {
        Thread.sleep(millis);
    }

    private void updateConfig(final KafkaClusterConfigSupplier kafkaClusterConfigSupplier) {
        if (kafkaClusterConfigSupplier != null) {
            if (sourceConfig.getBootstrapServers() == null) {
                sourceConfig.setBootStrapServers(kafkaClusterConfigSupplier.getBootStrapServers());
            }
            if (sourceConfig.getAuthConfig() == null) {
                sourceConfig.setAuthConfig(kafkaClusterConfigSupplier.getAuthConfig());
            }
            if (sourceConfig.getAwsConfig() == null) {
                sourceConfig.setAwsConfig(kafkaClusterConfigSupplier.getAwsConfig());
            }
            if (sourceConfig.getEncryptionConfigRaw() == null) {
                sourceConfig.setEncryptionConfig(kafkaClusterConfigSupplier.getEncryptionConfig());
            }
        }
    }
}
