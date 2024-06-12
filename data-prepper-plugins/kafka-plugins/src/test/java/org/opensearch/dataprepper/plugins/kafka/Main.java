package org.opensearch.dataprepper.plugins.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class Main {
    public static class UserRecord {
        @JsonProperty
        public String name;

        @JsonProperty
        public Integer id;

        @JsonProperty
        public Number value;

        public UserRecord() {}

        public UserRecord(String name, Integer id, Number value) {
            this.name = name;
            this.id = id;
            this.value = value;
        }
    };

    public static void createTopic(String servers, String username, String password, String topic) throws Throwable {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
//        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("ssl.endpoint.identification.algorithm", "https");
        props.put("security.protocol", "SASL_SSL");
        props.put("request.timeout.ms", 20000);
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("schema.registry.url", "https://psrc-e8157.us-east-2.aws.confluent.cloud");
        props.put("basic.auth.user.info", "7HTGQJCWT37CYCSA:O82vJ0cdCGIld0qVtOlIvnJEKuI2FEhlyZZRbnLrzunm0OjWEPfYpSZpRugmNpRN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""+ username +"\" password=\""+ password +"\";");
        Throwable[] createThrowable = new Throwable[1];
        try (AdminClient adminClient = AdminClient.create(props)) {
            // Create a new topic
            NewTopic newTopic = new NewTopic(topic, 1, (short) 3); // Topic name, numPartitions, replicationFactor
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            System.out.println("Topic created successfully.");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void describeTopic(String servers) throws Throwable {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
//        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("ssl.endpoint.identification.algorithm", "https");
        props.put("security.protocol", "SASL_SSL");
        props.put("request.timeout.ms", 20000);
        props.put("sasl.mechanism", "PLAIN");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("schema.registry.url", "https://psrc-m5k9x.us-west-2.aws.confluent.cloud");
        props.put("basic.auth.user.info", "6PB6XRBLRWMMJJRT:sJ/MUyz0dnWyKqiqGhLhBB5KkP94CKYRyplya3HpgUjF1tbEzgKvsS8xCUouqQbW");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""+"Q34YG5COE7EC6QUB"+"\" password=\""+"oXQaTRez85vfmt1beUW5cTGu+uCMprLxuswEC30cVEvm2mlLIFZ/xhtUBA8fhhXJ"+"\";");
        AtomicBoolean created = new AtomicBoolean(false);
        Throwable[] createThrowable = new Throwable[1];
        try (AdminClient adminClient = AdminClient.create(props)) {
            CreateTopicsResult createTopicsResult = adminClient.createTopics(
                    Collections.singleton(new NewTopic("topic_4", 1, (short) 1)));
            System.out.println(createTopicsResult.all().get());
        }
    }

    public static void produceJsonRecords(String topic, String servers, int numRecords, String username, String password) throws SerializationException, JsonProcessingException {
        final ObjectMapper objectMapper = new ObjectMapper();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
//        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("ssl.endpoint.identification.algorithm", "https");
        props.put("security.protocol", "SASL_SSL");
        props.put("request.timeout.ms", 20000);
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("schema.registry.url", "https://psrc-e8157.us-east-2.aws.confluent.cloud");
        props.put("basic.auth.user.info", "7HTGQJCWT37CYCSA:O82vJ0cdCGIld0qVtOlIvnJEKuI2FEhlyZZRbnLrzunm0OjWEPfYpSZpRugmNpRN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""+ username +"\" password=\""+ password +"\";");
        KafkaProducer producer = new KafkaProducer(props);
        for (int i = 0; i < numRecords; i++) {
            String key = "key"+String.valueOf(i);
            String testMessage = "M_"+RandomStringUtils.randomAlphabetic(5)+"_M_";
            UserRecord userRecord = new UserRecord(testMessage+i, i, (i+1));
            ProducerRecord<String, UserRecord> record = new ProducerRecord<String, UserRecord>(topic, key, userRecord);
            producer.send(record);
            try {
                Thread.sleep(100);
            } catch (Exception e) {
            }
        }
        producer.close();

    }

    public void produceAvroRecords(String topic, String servers, int numRecords, String username, String password) throws SerializationException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        properties.put("ssl.endpoint.identification.algorithm", "https");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("request.timeout.ms", 20000);
        properties.put("client.dns.lookup", "use_all_dns_ips");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        properties.put("basic.auth.credentials.source", "USER_INFO");
        properties.put("schema.registry.url", "https://psrc-e8157.us-east-2.aws.confluent.cloud");
        properties.put("basic.auth.user.info", "7HTGQJCWT37CYCSA:O82vJ0cdCGIld0qVtOlIvnJEKuI2FEhlyZZRbnLrzunm0OjWEPfYpSZpRugmNpRN");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""+ username +"\" password=\""+ password +"\";");

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(properties);
        String userSchema = "{\"type\":\"record\"," +
                "\"name\":\"sampleAvroRecord\"," +
                "\"fields\":[{\"name\":\"message\",\"type\":\"string\"}, {\"name\":\"ident\",\"type\":\"int\"}, {\"name\":\"score\",\"type\":\"double\"}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);
        for (int i = 0; i < numRecords; i++) {
            GenericRecord avroRecord = new GenericData.Record(schema);
            avroRecord.put("message", "M_"+RandomStringUtils.randomAlphabetic(5)+"_M_"+i);
            avroRecord.put("ident", i);
            avroRecord.put("score", (i+1));
            String key = "key"+String.valueOf(i);
            ProducerRecord<String, GenericRecord> record = new ProducerRecord<String, GenericRecord>(topic, key, avroRecord);
            producer.send(record);
        }
        producer.flush();
    }

    @Test
    void generateJSON() throws Throwable {
//        createTopic("pkc-rgm37.us-west-2.aws.confluent.cloud:9092", "Q5OE24C6PM4UQC5Q", "NqxLY3IB4or56QPxH5VIW0zTRRm6oBUzEpDFfJ0HEmXKmu7HT8toT/3ahhFFSDsJ");
        String topic = System.getenv("TOPIC");
        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");
        String username = System.getenv("USERNAME");
        String password = System.getenv("PASSWORD");
        produceJsonRecords(topic, bootstrapServers, 10,
                username, password);
    }

    @Test
    void generateAvro() throws Throwable {
//        createTopic("pkc-rgm37.us-west-2.aws.confluent.cloud:9092", "Q5OE24C6PM4UQC5Q", "NqxLY3IB4or56QPxH5VIW0zTRRm6oBUzEpDFfJ0HEmXKmu7HT8toT/3ahhFFSDsJ");
        String topic = System.getenv("TOPIC");
        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");
        String username = System.getenv("USERNAME");
        String password = System.getenv("PASSWORD");
        produceAvroRecords(topic, bootstrapServers, 10,
                username, password);
    }

    @Test
    void createTopic() throws Throwable {
        String topic = System.getenv("TOPIC");
        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");
        String username = System.getenv("USERNAME");
        String password = System.getenv("PASSWORD");
        createTopic(bootstrapServers, username, password, topic);
    }
}
