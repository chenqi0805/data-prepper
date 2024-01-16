package org.opensearch.dataprepper.plugins.kafka.configuration;

public class SchemaTypeReference {
    private String value;

    public SchemaTypeReference(final String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(final String newValue) {
        value = newValue;
    }
}
