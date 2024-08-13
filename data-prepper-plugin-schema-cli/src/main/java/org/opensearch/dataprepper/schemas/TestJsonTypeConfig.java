package org.opensearch.dataprepper.schemas;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestJsonTypeConfig {
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.WRAPPER_OBJECT, visible = false) @JsonSubTypes({

            @JsonSubTypes.Type(value = Square.class, name = "square"),
            @JsonSubTypes.Type(value = Circle.class, name = "circle")
    })
    static class Shape {
        public String name;
        Shape(String name){
            this.name = name;
        }
    }
    @JsonTypeName("square")
    static class Square extends Shape {
        public double length;
        Square(){
            this(null,0.0);
        }
        Square(String name, double length){
            super(name);
            this.length = length;
        }
    }
    @JsonTypeName("circle")
    static class Circle extends Shape {
        public double radius;
        Circle(){
            this(null,0.0);
        }
        Circle(String name, double radius){
            super(name);
            this.radius = radius;
        }
    }

    static class Wrapper {
        public Shape shape;
    }

    public static void main(String[] args) throws JsonProcessingException {
        Shape shape = new Circle("CustomCircle", 1);
        String result = new ObjectMapper()
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(shape);
        System.out.println(result);
        String json = "{\"shape\":{\"circle\": {\"name\":\"CustomCircle\",\"radius\":1.0}}}";
        Wrapper shape1 = new ObjectMapper().readValue(json, Wrapper.class);
        System.out.println(shape1.shape);
    }
}
