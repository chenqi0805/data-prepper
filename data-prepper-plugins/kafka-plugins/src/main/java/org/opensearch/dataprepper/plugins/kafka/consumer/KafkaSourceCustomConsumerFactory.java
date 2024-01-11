package org.opensearch.dataprepper.plugins.kafka.consumer;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.OAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaRegistryType;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.util.ClientDNSLookupType;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSecurityConfigurer;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaTopicConsumerMetrics;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaSourceCustomConsumerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceCustomConsumerFactory.class);

    private String schemaType = MessageFormat.PLAINTEXT.toString();

    private final StringDeserializer stringDeserializer = new StringDeserializer();

    public KafkaCustomConsumer refreshConsumerForTopic(final KafkaCustomConsumer existingKafkaCustomConsumer,
                                                       final KafkaConsumerConfig kafkaConsumerConfig,
                                                       final KafkaTopicConsumerMetrics topicMetrics,
                                                       final AtomicBoolean shutdownInProgress,
                                                       final AcknowledgementSetManager acknowledgementSetManager) {
        Properties authProperties = new Properties();
        KafkaSecurityConfigurer.setAuthProperties(authProperties, kafkaConsumerConfig, LOG);
        Properties consumerProperties = getConsumerProperties(kafkaConsumerConfig,
                existingKafkaCustomConsumer.getTopicConfig(), authProperties);
        MessageFormat schema = MessageFormat.getByMessageFormatByName(schemaType);

        final KafkaConsumer kafkaConsumer = createKafkaConsumer(kafkaConsumerConfig, schema, consumerProperties);
        return new KafkaCustomConsumer(
                kafkaConsumer,
                shutdownInProgress,
                existingKafkaCustomConsumer.getBuffer(),
                kafkaConsumerConfig,
                existingKafkaCustomConsumer.getTopicConfig(),
                schemaType,
                acknowledgementSetManager,
                existingKafkaCustomConsumer.getByteDecoder(),
                topicMetrics,
                PauseConsumePredicate.noPause(),
                existingKafkaCustomConsumer.getOffsetsToCommit(),
                existingKafkaCustomConsumer.getAcknowledgedOffsets(),
                existingKafkaCustomConsumer.getOwnedPartitionsEpoch(),
                existingKafkaCustomConsumer.getMetricsUpdatedTime(),
                existingKafkaCustomConsumer.getPartitionCommitTrackerMap(),
                existingKafkaCustomConsumer.getPartitionsToReset(),
                existingKafkaCustomConsumer.getBufferAccumulator(),
                existingKafkaCustomConsumer.getLastCommitTime(),
                existingKafkaCustomConsumer.getNumberOfAcksPending(),
                existingKafkaCustomConsumer.getErrLogRateLimiter(),
                existingKafkaCustomConsumer.getNumRecordsCommitted());
    }

    public KafkaCustomConsumer createConsumerForTopic(final KafkaConsumerConfig kafkaConsumerConfig,
                                                      final TopicConsumerConfig topic,
                                                      final KafkaTopicConsumerMetrics topicMetrics,
                                                      final AtomicBoolean shutdownInProgress,
                                                      final Buffer<Record<Event>> buffer,
                                                      final AcknowledgementSetManager acknowledgementSetManager) {
        Properties authProperties = new Properties();
        KafkaSecurityConfigurer.setAuthProperties(authProperties, kafkaConsumerConfig, LOG);
        Properties consumerProperties = getConsumerProperties(kafkaConsumerConfig, topic, authProperties);
        MessageFormat schema = MessageFormat.getByMessageFormatByName(schemaType);

        final KafkaConsumer kafkaConsumer = createKafkaConsumer(kafkaConsumerConfig, schema, consumerProperties);
        return new KafkaCustomConsumer(kafkaConsumer, shutdownInProgress, buffer, kafkaConsumerConfig, topic,
                schemaType, acknowledgementSetManager, null, topicMetrics, PauseConsumePredicate.noPause());
    }

    private KafkaConsumer<?, ?> createKafkaConsumer(final KafkaConsumerConfig kafkaConsumerConfig,
                                                   final MessageFormat schema,
                                                   final Properties consumerProperties) {
        switch (schema) {
            case JSON:
                return new KafkaConsumer<String, JsonNode>(consumerProperties);
            case AVRO:
                return new KafkaConsumer<String, GenericRecord>(consumerProperties);
            case PLAINTEXT:
            default:
                final GlueSchemaRegistryKafkaDeserializer glueDeserializer = KafkaSecurityConfigurer
                        .getGlueSerializer(kafkaConsumerConfig);
                if (Objects.nonNull(glueDeserializer)) {
                    return new KafkaConsumer(consumerProperties, stringDeserializer, glueDeserializer);
                } else {
                    return new KafkaConsumer<String, String>(consumerProperties);
                }
        }
    }

    private Properties getConsumerProperties(final KafkaConsumerConfig kafkaConsumerConfig, final TopicConsumerConfig topicConfig, final Properties authProperties) {
        Properties properties = (Properties) authProperties.clone();
        if (StringUtils.isNotEmpty(kafkaConsumerConfig.getClientDnsLookup())) {
            ClientDNSLookupType dnsLookupType = ClientDNSLookupType.getDnsLookupType(kafkaConsumerConfig.getClientDnsLookup());
            switch (dnsLookupType) {
                case USE_ALL_DNS_IPS:
                    properties.put("client.dns.lookup", ClientDNSLookupType.USE_ALL_DNS_IPS.toString());
                    break;
                case CANONICAL_BOOTSTRAP:
                    properties.put("client.dns.lookup", ClientDNSLookupType.CANONICAL_BOOTSTRAP.toString());
                    break;
                case DEFAULT:
                    properties.put("client.dns.lookup", ClientDNSLookupType.DEFAULT.toString());
                    break;
            }
        }
        setConsumerTopicProperties(properties, topicConfig);
        setSchemaRegistryProperties(kafkaConsumerConfig, properties, topicConfig);
        LOG.info("Starting consumer with the properties : {}", properties);
        return properties;
    }

    private void setConsumerTopicProperties(Properties properties, TopicConsumerConfig topicConfig) {
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, topicConfig.getGroupId());
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, (int) topicConfig.getMaxPartitionFetchBytes());
        properties.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, ((Long) topicConfig.getRetryBackoff().toMillis()).intValue());
        properties.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, ((Long) topicConfig.getReconnectBackoff().toMillis()).intValue());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                topicConfig.getAutoCommit());
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                ((Long) topicConfig.getCommitInterval().toMillis()).intValue());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                topicConfig.getAutoOffsetReset());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                topicConfig.getConsumerMaxPollRecords());
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                ((Long) topicConfig.getMaxPollInterval().toMillis()).intValue());
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, ((Long) topicConfig.getSessionTimeOut().toMillis()).intValue());
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, ((Long) topicConfig.getHeartBeatInterval().toMillis()).intValue());
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, (int) topicConfig.getFetchMaxBytes());
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, topicConfig.getFetchMaxWait());
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, (int) topicConfig.getFetchMinBytes());
    }

    private void setSchemaRegistryProperties(final KafkaConsumerConfig kafkaConsumerConfig,
                                             final Properties properties,
                                             final TopicConfig topicConfig) {
        SchemaConfig schemaConfig = kafkaConsumerConfig.getSchemaConfig();
        if (Objects.isNull(schemaConfig)) {
            setPropertiesForPlaintextAndJsonWithoutSchemaRegistry(properties, topicConfig);
            return;
        }

        if (schemaConfig.getType() == SchemaRegistryType.AWS_GLUE) {
            return;
        }

        /* else schema registry type is Confluent */
        if (StringUtils.isNotEmpty(schemaConfig.getRegistryURL())) {
            setPropertiesForSchemaRegistryConnectivity(kafkaConsumerConfig, properties);
            setPropertiesForSchemaType(kafkaConsumerConfig, properties, topicConfig);
        } else {
            throw new RuntimeException("RegistryURL must be specified for confluent schema registry");
        }
    }

    private void setPropertiesForPlaintextAndJsonWithoutSchemaRegistry(Properties properties, final TopicConfig topicConfig) {
        MessageFormat dataFormat = topicConfig.getSerdeFormat();
        schemaType = dataFormat.toString();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        switch (dataFormat) {
            case JSON:
                properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
                break;
            default:
            case PLAINTEXT:
                properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        StringDeserializer.class);
                break;
        }
    }

    private void setPropertiesForSchemaType(final KafkaConsumerConfig kafkaConsumerConfig,
                                            final Properties properties,
                                            final TopicConfig topic) {
        Map prop = properties;
        Map<String, String> propertyMap = (Map<String, String>) prop;
        final SchemaConfig schemaConfig = kafkaConsumerConfig.getSchemaConfig();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("schema.registry.url", schemaConfig.getRegistryURL());
        properties.put("auto.register.schemas", false);
        final CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(
                schemaConfig.getRegistryURL(), 100, propertyMap);
        try {
            schemaType = schemaRegistryClient.getSchemaMetadata(topic.getName() + "-value",
                    kafkaConsumerConfig.getSchemaConfig().getVersion()).getSchemaType();
        } catch (IOException | RestClientException e) {
            LOG.error("Failed to connect to the schema registry...");
            throw new RuntimeException(e);
        }
        if (schemaType.equalsIgnoreCase(MessageFormat.JSON.toString())) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class);
            properties.put("json.value.type", "com.fasterxml.jackson.databind.JsonNode");
        } else if (schemaType.equalsIgnoreCase(MessageFormat.AVRO.toString())) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        } else {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class);
        }
    }

    private void setPropertiesForSchemaRegistryConnectivity(final KafkaConsumerConfig kafkaConsumerConfig,
                                                            final Properties properties) {
        AuthConfig authConfig = kafkaConsumerConfig.getAuthConfig();
        String schemaRegistryApiKey = kafkaConsumerConfig.getSchemaConfig().getSchemaRegistryApiKey();
        String schemaRegistryApiSecret = kafkaConsumerConfig.getSchemaConfig().getSchemaRegistryApiSecret();
        //with plaintext authentication for schema registry
        if ("USER_INFO".equalsIgnoreCase(kafkaConsumerConfig.getSchemaConfig().getBasicAuthCredentialsSource())
                && authConfig.getSaslAuthConfig().getPlainTextAuthConfig() != null) {
            String schemaBasicAuthUserInfo = schemaRegistryApiKey.concat(":").concat(schemaRegistryApiSecret);
            properties.put("basic.auth.user.info", schemaBasicAuthUserInfo);
            properties.put("basic.auth.credentials.source", "USER_INFO");
        }

        if (authConfig != null && authConfig.getSaslAuthConfig() != null) {
            PlainTextAuthConfig plainTextAuthConfig = authConfig.getSaslAuthConfig().getPlainTextAuthConfig();
            OAuthConfig oAuthConfig = authConfig.getSaslAuthConfig().getOAuthConfig();
            if (oAuthConfig != null) {
                properties.put("sasl.mechanism", oAuthConfig.getOauthSaslMechanism());
                properties.put("security.protocol", oAuthConfig.getOauthSecurityProtocol());
            }
        }
    }
}
