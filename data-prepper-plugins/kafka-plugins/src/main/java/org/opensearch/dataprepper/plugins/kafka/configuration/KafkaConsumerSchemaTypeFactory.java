package org.opensearch.dataprepper.plugins.kafka.configuration;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class KafkaConsumerSchemaTypeFactory {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerSchemaTypeFactory.class);
    static final MessageFormat DEFAULT_SCHEMA_TYPE = MessageFormat.PLAINTEXT;
    public MessageFormat deriveSchemaType(final KafkaConsumerConfig kafkaConsumerConfig,
                                          final TopicConfig topicConfig) {
        SchemaConfig schemaConfig = kafkaConsumerConfig.getSchemaConfig();
        if (Objects.isNull(schemaConfig)) {
            return topicConfig.getSerdeFormat();
        }

        if (schemaConfig.getType() == SchemaRegistryType.AWS_GLUE) {
            return DEFAULT_SCHEMA_TYPE;
        }

        if (StringUtils.isNotEmpty(schemaConfig.getRegistryURL())) {
            final Properties schemaRegistryProperties = new Properties();
            setPropertiesForSchemaRegistry(kafkaConsumerConfig, schemaRegistryProperties);
            final Map prop = schemaRegistryProperties;
            Map<String, String> propertyMap = (Map<String, String>) prop;
            final CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(
                    schemaConfig.getRegistryURL(), 100, propertyMap);
            final String schemaType;
            try {
                schemaType = schemaRegistryClient.getSchemaMetadata(topicConfig.getName() + "-value",
                        kafkaConsumerConfig.getSchemaConfig().getVersion()).getSchemaType();
                return MessageFormat.getByMessageFormatByName(schemaType);
            } catch (IOException | RestClientException e) {
                LOG.error("Failed to connect to the schema registry...");
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("RegistryURL must be specified for confluent schema registry");
        }
    }

    private void setPropertiesForSchemaRegistry(final KafkaConsumerConfig kafkaConsumerConfig,
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
            OAuthConfig oAuthConfig = authConfig.getSaslAuthConfig().getOAuthConfig();
            if (oAuthConfig != null) {
                properties.put("sasl.mechanism", oAuthConfig.getOauthSaslMechanism());
                properties.put("security.protocol", oAuthConfig.getOauthSecurityProtocol());
            }
        }
        final SchemaConfig schemaConfig = kafkaConsumerConfig.getSchemaConfig();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("schema.registry.url", schemaConfig.getRegistryURL());
        properties.put("auto.register.schemas", false);
    }
}
