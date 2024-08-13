package org.opensearch.dataprepper.schemas;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface DependsOn {
    String[] keyValuePatterns() default {};
}
