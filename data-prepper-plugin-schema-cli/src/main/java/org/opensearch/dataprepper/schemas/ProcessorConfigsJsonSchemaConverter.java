package org.opensearch.dataprepper.schemas;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.victools.jsonschema.generator.MemberScope;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;
import com.github.victools.jsonschema.module.jackson.JacksonModule;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.configuration.ConditionalRoute;
import org.opensearch.dataprepper.model.processor.Processor;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ProcessorConfigsJsonSchemaConverter {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessorConfigsJsonSchemaConverter.class);
    private final JsonSchemaConverter jsonSchemaConverter;

    public ProcessorConfigsJsonSchemaConverter(
            final JsonSchemaConverter jsonSchemaConverter) {
        this.jsonSchemaConverter = jsonSchemaConverter;
    }

    public Map<String, String> convertProcessorsIntoJsonSchemas() {
        final Map<String, Class<?>> nameToConfigClass = scanForProcessorConfigs();
        return nameToConfigClass.entrySet().stream()
                .map(entry -> {
                    String value = null;
                    try {
                        final ObjectNode schemaNode = jsonSchemaConverter.convertIntoJsonSchema(
                                SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON, entry.getValue());
                        schemaNode.put("documentation",
                                String.format(
                                        "https://opensearch.org/docs/latest/data-prepper/pipelines/configuration/processors/%s/",
                                        entry.getKey()));
                        value = schemaNode.toPrettyString();
                    } catch (JsonProcessingException e) {
                        LOG.error("Encountered error retrieving JSON schema for {}", entry.getValue());
                    }
                    return new AbstractMap.SimpleEntry<>(entry.getKey(), value);
                })
                .filter(entry -> Objects.nonNull(entry.getValue()))
                .collect(Collectors.toMap(
                        AbstractMap.SimpleEntry::getKey,
                        AbstractMap.SimpleEntry::getValue
                ));
    }

    public Map<String, Class<?>> scanForProcessorConfigs() {
        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage("org.opensearch.dataprepper.plugins"))
                .setScanners(Scanners.TypesAnnotated));

        final Map<String, Class<?>> result = new HashMap<>();
        result.put("conditional_route", ConditionalRoute.class);

        result.putAll(
                reflections.getTypesAnnotatedWith(DataPrepperPlugin.class).stream()
                        .map(clazz -> clazz.getAnnotation(DataPrepperPlugin.class))
                        .filter(dataPrepperPlugin -> Processor.class.equals(dataPrepperPlugin.pluginType()))
                        .collect(Collectors.toMap(
                                DataPrepperPlugin::name,
                                DataPrepperPlugin::pluginConfigurationType
                        ))
        );

        return result;
    }

    public static void main(String[] args) throws JsonProcessingException {
//        final List<Module> modules = List.of(
//                new JacksonModule(RESPECT_JSONPROPERTY_REQUIRED),
//                new JakartaValidationModule(JakartaValidationOption.NOT_NULLABLE_FIELD_IS_REQUIRED,
//                        JakartaValidationOption.INCLUDE_PATTERN_EXPRESSIONS)
//        );
////        System.out.println(new ProcessorConfigsJsonSchemaConverter(new JsonSchemaConverter(modules))
////                .convertProcessorsIntoJsonSchemas());
//        System.out.println(new JsonSchemaConverter(modules).convertIntoJsonSchema(
//                SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON, TestJsonSchemaPluginConfig.class).toPrettyString());

        SchemaGeneratorConfigBuilder configBuilder = new SchemaGeneratorConfigBuilder(
                SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON
        );
        JacksonModule jacksonModule = new CustomJacksonModule();
        configBuilder = configBuilder.with(jacksonModule);

        SchemaGenerator generator = new SchemaGenerator(configBuilder.build());

        // Example class to generate schema for

        // Generate schema
        ObjectNode jsonSchema = generator.generateSchema(ExampleClass.class);
        System.out.println(jsonSchema.toPrettyString());
        System.out.println(new ObjectMapper().convertValue(Map.of("myFieldName", "sth"), ExampleClass.class));
    }

    static class ExampleClass {
        private String myFieldName;

        public String getMyFieldName() {
            return myFieldName;
        }
    }

    static class CustomJacksonModule extends JacksonModule {
        @Override
        protected String getPropertyNameOverrideBasedOnJsonPropertyAnnotation(MemberScope<?, ?> member) {
            JsonProperty annotation = member.getAnnotationConsideringFieldAndGetter(JsonProperty.class);
            if (annotation != null) {
                String nameOverride = annotation.value();
                // check for invalid overrides
                if (nameOverride != null && !nameOverride.isEmpty() && !nameOverride.equals(member.getDeclaredName())) {
                    return nameOverride;
                }
            }
            return PropertyNamingStrategies.SNAKE_CASE.nameForField(null, null, member.getName());
        }
    }
}
