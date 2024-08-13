package org.opensearch.dataprepper.schemas;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.model.event.DataType;

import java.util.List;
import java.util.Map;

@JsonClassDescription(value = "https://opensearch.org/docs/latest/data-prepper/pipelines/configuration/processors/obfuscate/#configuration")
public class TestJsonSchemaPluginConfig {

    @JsonProperty(value = "name_1", required = true)
    @JsonPropertyDescription("The name of the person")
    private String name;

    @JsonProperty(value = "age_1")
    @JsonPropertyDescription("The age of the person in years")
    @NotNull
    private int age;

    @JsonAnySetter
    private Map<String, Object> extensionMap;

    @Min(0)
    @JsonProperty(value = "number_1")
    private int number;

    @JsonProperty(value = "custom_name", defaultValue = "sth")
    private String custom;

    @Deprecated
    private String deprecated;

    private Map<String, ActionConfig> attributes;

    private ActionConfig actionConfig;

    @Valid
    private List<ActionConfig> actionConfigs;

    @Pattern(regexp = ".+?\\..+")
    private String pattern;

    @JsonAnyGetter
    public Map<String, Object> getExtensionMap() {
        return extensionMap;
    }

    public String getPattern() {
        return pattern;
    }

    public ActionModel getActionModel() {
        return actionModel;
    }

    private ActionModel actionModel;

    @Size(max = 5)
    List<String> sizedList;

    @NotEmpty
    private String notEmpty;

    @NotEmpty
    private List<String> notEmptyList;

    @Size(min = 1, message = "must be at least of length 1.")
    private String sizedString;

    @Email
    private String email;

    @DecimalMax(value = "100.00", inclusive = false, message = "Price must be less than 100.00")
    private double decimal;

    @DecimalMin(value = "1.00", inclusive = false, message = "Price must be larger than 1.00")
    private float decimalMin;

    private TestEnum testEnum;

    public List<ActionConfig> getActionConfigs() {
        return actionConfigs;
    }

    public ActionConfig getActionConfig() {
        return actionConfig;
    }

    public Map<String, ActionConfig> getAttributes() {
        return attributes;
    }

    public TestEnum getTestEnum() {
        return testEnum;
    }

    public double getDecimal() {
        return decimal;
    }

    public DataType getDataType() {
        return dataType;
    }

    public String getSizedString() {
        return sizedString;
    }

    private DataType dataType;

    @DependsOn(keyValuePatterns = {"testEnum:VALUE_1"})
    private String dependsOn;

    @JsonProperty("schema")
    private String schema;

    @JsonProperty("auto_schema")
    private boolean autoSchema;

    @AssertTrue(message = "Working must be true")
    private boolean working;

    public boolean isWorking() {
        return working;
    }

    @AssertTrue(message = "The Avro codec requires either defining a schema or setting auto_schema to true to automatically generate a schema.")
    boolean isSchemaOrAutoSchemaDefined() {
        return schema != null ^ autoSchema;
    }

    public String getDependsOn() {
        return dependsOn;
    }

    // Getters and setters

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public List<String> getSizedList() {
        return sizedList;
    }

    public String getNotEmpty() {
        return notEmpty;
    }

    public String getEmail() {
        return email;
    }

    public String getDeprecated() {
        return deprecated;
    }

    public List<String> getNotEmptyList() {
        return notEmptyList;
    }
}
