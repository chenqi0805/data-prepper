package org.opensearch.dataprepper.plugins.kafka.consumer;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
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
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;

public class KafkaConsumerFactory {
    private static final String NO_RESOLVABLE_URLS_ERROR_MESSAGE = "No resolvable bootstrap urls given in bootstrap.servers";
    private static final long RETRY_SLEEP_INTERVAL = 30000;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerFactory.class);
    private final StringDeserializer stringDeserializer = new StringDeserializer();
    private final MessageFormat schema;

    public KafkaConsumerFactory(final MessageFormat schema) {
        this.schema = schema;
    }

    public KafkaConsumer<?, ?> refreshKafkaConsumer(final KafkaConsumer existingConsumer,
                                                    final KafkaConsumerConfig kafkaConsumerConfig,
                                                    final TopicConsumerConfig topic) {
        final KafkaConsumer newConsumer = createKafkaConsumer(kafkaConsumerConfig, topic);
        transferInternalState(existingConsumer, newConsumer);
        return newConsumer;
    }

    private void transferInternalState(final KafkaConsumer existingConsumer,
                                       final KafkaConsumer newConsumer) {
        final Collection<TopicPartition> assignments = existingConsumer.assignment();
        final Map<TopicPartition, Long> offsets = assignments.stream()
                .collect(Collectors.toMap(Function.identity(), existingConsumer::position));
        newConsumer.assign(assignments);
        offsets.forEach(newConsumer::seek);
    }

    public KafkaConsumer<?, ?> createKafkaConsumer(final KafkaConsumerConfig kafkaConsumerConfig,
                                                   final TopicConsumerConfig topic) {
        Properties authProperties = new Properties();
        KafkaSecurityConfigurer.setAuthProperties(authProperties, kafkaConsumerConfig, LOG);
        Properties consumerProperties = getConsumerProperties(
                kafkaConsumerConfig, topic, authProperties);

        while (true) {
            try {
                return createKafkaConsumer(kafkaConsumerConfig, schema, consumerProperties);
            } catch (ConfigException ce) {
                if (ce.getMessage().contains(NO_RESOLVABLE_URLS_ERROR_MESSAGE)) {
                    LOG.warn("Exception while creating Kafka consumer: ", ce);
                    LOG.warn("Bootstrap URL could not be resolved. Retrying in {} ms...", RETRY_SLEEP_INTERVAL);
                    try {
                        sleep(RETRY_SLEEP_INTERVAL);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(ie);
                    }
                } else {
                    throw ce;
                }
            }
        }
    }

    private KafkaConsumer createKafkaConsumer(final KafkaConsumerConfig kafkaConsumerConfig,
                                              final MessageFormat schema, final Properties consumerProperties) {
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

    private Properties getConsumerProperties(final KafkaConsumerConfig kafkaConsumerConfig,
                                             final TopicConsumerConfig topicConfig,
                                             final Properties authProperties) {
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
            setPropertiesForPlaintextAndJsonWithoutSchemaRegistry(properties);
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

    private void setPropertiesForPlaintextAndJsonWithoutSchemaRegistry(
            Properties properties) {
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        switch (schema) {
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
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        if (schema.equals(MessageFormat.JSON)) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class);
            properties.put("json.value.type", "com.fasterxml.jackson.databind.JsonNode");
        } else if (schema.equals(MessageFormat.AVRO.toString())) {
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
