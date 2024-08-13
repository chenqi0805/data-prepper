package org.opensearch.dataprepper.schemas;

import com.github.victools.jsonschema.generator.Module;
import com.github.victools.jsonschema.module.jackson.JacksonModule;
import com.github.victools.jsonschema.module.jakarta.validation.JakartaValidationModule;
import com.github.victools.jsonschema.module.jakarta.validation.JakartaValidationOption;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static com.github.victools.jsonschema.module.jackson.JacksonOption.RESPECT_JSONPROPERTY_REQUIRED;

public class MapToJsonFiles {
    public static void writeMapToFiles(Map<String, String> map, String folderPath) {
        // Ensure the directory exists
        Path directory = Paths.get(folderPath);
        if (!Files.exists(directory)) {
            try {
                Files.createDirectories(directory);
            } catch (IOException e) {
                System.err.println("Error creating directory: " + e.getMessage());
                return;
            }
        }

        // Iterate through the map and write each entry to a file
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String fileName = entry.getKey() + ".json";
            Path filePath = directory.resolve(fileName);

            try {
                Files.write(filePath, entry.getValue().getBytes());
                System.out.println("Written file: " + filePath);
            } catch (IOException e) {
                System.err.println("Error writing file " + fileName + ": " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        final List<Module> modules = List.of(
                new JacksonModule(RESPECT_JSONPROPERTY_REQUIRED),
                new JakartaValidationModule(JakartaValidationOption.NOT_NULLABLE_FIELD_IS_REQUIRED,
                        JakartaValidationOption.INCLUDE_PATTERN_EXPRESSIONS)
        );
        // Example usage
        Map<String, String> map = new ProcessorConfigsJsonSchemaConverter(new JsonSchemaConverter(modules))
                .convertProcessorsIntoJsonSchemas();

        String folderPath = "/Users/qchea/Documents/data-prepper-processor-json-schemas";
        writeMapToFiles(map, folderPath);
    }
}
