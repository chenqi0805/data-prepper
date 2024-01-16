package org.opensearch.dataprepper.plugins.kafka.configuration;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class SchemaTypeReferenceTest {
    private static final String TEST_SCHEMA_TYPE = UUID.randomUUID().toString();
    private final SchemaTypeReference objectUnderTest = new SchemaTypeReference(TEST_SCHEMA_TYPE);

    @Test
    void getValue() {
        assertThat(objectUnderTest.getValue(), equalTo(TEST_SCHEMA_TYPE));
    }

    @Test
    void setValue() {
        final String newSchemaType = TEST_SCHEMA_TYPE + "-diff";
        objectUnderTest.setValue(newSchemaType);
        assertThat(objectUnderTest.getValue(), equalTo(newSchemaType));
    }
}