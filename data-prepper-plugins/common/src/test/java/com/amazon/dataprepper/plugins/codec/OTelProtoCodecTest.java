/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.codec;

import com.amazon.dataprepper.model.trace.DefaultTraceGroupFields;
import com.amazon.dataprepper.model.trace.Span;
import com.amazon.dataprepper.model.trace.TraceGroupFields;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.common.v1.KeyValueList;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Status;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

public class OTelProtoCodecTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String TEST_REQUEST_JSON_FILE = "test-request.json";

    private Map<String, Object> returnMap(final String jsonStr) throws JsonProcessingException {
        return (Map<String, Object>) OBJECT_MAPPER.readValue(jsonStr, Map.class);
    }

    private List<Object> returnList(final String jsonStr) throws JsonProcessingException {
        return (List<Object>) OBJECT_MAPPER.readValue(jsonStr, List.class);
    }

    private static ExportTraceServiceRequest buildExportTraceServiceRequestFromJsonFile(String requestJsonFileName) throws IOException {
        final StringBuilder jsonBuilder = new StringBuilder();
        try (final InputStream inputStream = Objects.requireNonNull(
                OTelProtoCodecTest.class.getClassLoader().getResourceAsStream(requestJsonFileName))){
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            bufferedReader.lines().forEach(jsonBuilder::append);
        }
        final String requestJson = jsonBuilder.toString();
        final ExportTraceServiceRequest.Builder builder = ExportTraceServiceRequest.newBuilder();
        JsonFormat.parser().merge(requestJson, builder);
        return builder.build();
    }

    @Test
    public void testParseExportTraceServiceRequest() throws IOException {
        final ExportTraceServiceRequest exportTraceServiceRequest = buildExportTraceServiceRequestFromJsonFile(TEST_REQUEST_JSON_FILE);
        final List<Span> spans = OTelProtoCodec.parseExportTraceServiceRequest(exportTraceServiceRequest);
        assertThat(spans.size(), is(equalTo(3)));
        for (final Span span: spans) {
            if (span.getParentSpanId().isEmpty()) {
                assertThat(span.getTraceGroup(), notNullValue());
                assertThat(span.getTraceGroupFields().getEndTime(), notNullValue());
                assertThat(span.getTraceGroupFields().getDurationInNanos(), notNullValue());
                assertThat(span.getTraceGroupFields().getStatusCode(), notNullValue());
            } else {
                assertThat(span.getTraceGroup(), nullValue());
                assertThat(span.getTraceGroupFields().getEndTime(), nullValue());
                assertThat(span.getTraceGroupFields().getDurationInNanos(), nullValue());
                assertThat(span.getTraceGroupFields().getStatusCode(), nullValue());
            }
            Map<String, Object> attributes = span.getAttributes();
            assertThat(attributes.containsKey(OTelProtoCodec.RESOURCE_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply("service.name")), is(true));
            assertThat(attributes.containsKey(OTelProtoCodec.INSTRUMENTATION_LIBRARY_NAME), is(true));
            assertThat(attributes.containsKey(OTelProtoCodec.STATUS_CODE), is(true));
        }
    }

    /**
     * Below object has a KeyValue with a key mapped to KeyValueList and is part of the span attributes
     *
     * @throws JsonProcessingException
     */
    @Test
    public void testKeyValueListAsSpanAttributes() throws JsonProcessingException {

        final KeyValue childAttr1 = KeyValue.newBuilder().setKey("statement").setValue(AnyValue.newBuilder()
                .setIntValue(1_000).build()).build();
        final KeyValue childAttr2 = KeyValue.newBuilder().setKey("statement.params").setValue(AnyValue.newBuilder()
                .setStringValue("us-east-1").build()).build();
        final KeyValue spanAttribute1 = KeyValue.newBuilder().setKey("db.details").setValue(AnyValue.newBuilder()
                .setKvlistValue(KeyValueList.newBuilder().addAllValues(Arrays.asList(childAttr1, childAttr2)).build()).build()).build();
        final KeyValue spanAttribute2 = KeyValue.newBuilder().setKey("http.status").setValue(AnyValue.newBuilder()
                .setStringValue("4xx").build()).build();

        final Map<String, Object> actual = OTelProtoCodec.getSpanAttributes(io.opentelemetry.proto.trace.v1.Span.newBuilder()
                .addAllAttributes(Arrays.asList(spanAttribute1, spanAttribute2)).build());
        assertThat(actual.get(OTelProtoCodec.SPAN_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(spanAttribute2.getKey())),
                equalTo(spanAttribute2.getValue().getStringValue()));
        assertThat(actual.containsKey(OTelProtoCodec.SPAN_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(spanAttribute1.getKey())), is(true));
        final Map<String, Object> actualValue = returnMap((String) actual
                .get(OTelProtoCodec.SPAN_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(spanAttribute1.getKey())));
        assertThat(((Number) actualValue.get(OTelProtoCodec.REPLACE_DOT_WITH_AT.apply(childAttr1.getKey()))).longValue(),
                equalTo(childAttr1.getValue().getIntValue()));
        assertThat(actualValue.get(OTelProtoCodec.REPLACE_DOT_WITH_AT.apply(childAttr2.getKey())), equalTo(childAttr2.getValue().getStringValue()));
    }

    /**
     * Below object has a KeyValue with a key mapped to KeyValueList and is part of the resource attributes
     *
     * @throws JsonProcessingException
     */
    @Test
    public void testKeyValueListAsResourceAttributes() throws JsonProcessingException {
        final KeyValue childAttr1 = KeyValue.newBuilder().setKey("ec2.instances").setValue(AnyValue.newBuilder()
                .setIntValue(20).build()).build();
        final KeyValue childAttr2 = KeyValue.newBuilder().setKey("ec2.instance.az").setValue(AnyValue.newBuilder()
                .setStringValue("us-east-1").build()).build();
        final KeyValue spanAttribute1 = KeyValue.newBuilder().setKey("aws.details").setValue(AnyValue.newBuilder()
                .setKvlistValue(KeyValueList.newBuilder().addAllValues(Arrays.asList(childAttr1, childAttr2)).build()).build()).build();
        final KeyValue spanAttribute2 = KeyValue.newBuilder().setKey("service.name").setValue(AnyValue.newBuilder()
                .setStringValue("EaglesService").build()).build();

        final Map<String, Object> actual = OTelProtoCodec.getResourceAttributes(Resource.newBuilder()
                .addAllAttributes(Arrays.asList(spanAttribute1, spanAttribute2)).build());
        assertThat(actual.get(OTelProtoCodec.RESOURCE_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(spanAttribute2.getKey())),
                equalTo(spanAttribute2.getValue().getStringValue()));
        assertThat(actual.containsKey(OTelProtoCodec.RESOURCE_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(spanAttribute1.getKey())), is(true));
        final Map<String, Object> actualValue = returnMap((String) actual
                .get(OTelProtoCodec.RESOURCE_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(spanAttribute1.getKey())));
        assertThat(((Number) actualValue.get(OTelProtoCodec.REPLACE_DOT_WITH_AT.apply(childAttr1.getKey()))).longValue(), equalTo(childAttr1.getValue().getIntValue()));
        assertThat(actualValue.get(OTelProtoCodec.REPLACE_DOT_WITH_AT.apply(childAttr2.getKey())), equalTo(childAttr2.getValue().getStringValue()));

    }


    /**
     * Below object has a KeyValue with a key mapped to KeyValueList and is part of the span attributes
     *
     * @throws JsonProcessingException
     */
    @Test
    public void testArrayOfValueAsResourceAttributes() throws JsonProcessingException {
        final KeyValue childAttr1 = KeyValue.newBuilder().setKey("ec2.instances").setValue(AnyValue.newBuilder()
                .setIntValue(20).build()).build();
        final KeyValue childAttr2 = KeyValue.newBuilder().setKey("ec2.instance.az").setValue(AnyValue.newBuilder()
                .setStringValue("us-east-1").build()).build();
        final AnyValue anyValue1 = AnyValue.newBuilder().setStringValue(UUID.randomUUID().toString()).build();
        final AnyValue anyValue2 = AnyValue.newBuilder().setDoubleValue(2000.123).build();
        final AnyValue anyValue3 = AnyValue.newBuilder().setKvlistValue(KeyValueList.newBuilder().addAllValues(Arrays.asList(childAttr1, childAttr2))).build();
        final ArrayValue arrayValue = ArrayValue.newBuilder().addAllValues(Arrays.asList(anyValue1, anyValue2, anyValue3)).build();
        final KeyValue spanAttribute1 = KeyValue.newBuilder().setKey("aws.details").setValue(AnyValue.newBuilder()
                .setArrayValue(arrayValue)).build();

        final Map<String, Object> actual = OTelProtoCodec.getResourceAttributes(Resource.newBuilder()
                .addAllAttributes(Collections.singletonList(spanAttribute1)).build());
        assertThat(actual.containsKey(OTelProtoCodec.RESOURCE_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(spanAttribute1.getKey())), is(true));
        final List<Object> actualValue = returnList((String) actual
                .get(OTelProtoCodec.RESOURCE_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(spanAttribute1.getKey())));
        assertThat(actualValue.get(0), equalTo(anyValue1.getStringValue()));
        assertThat(((Double) actualValue.get(1)), equalTo(anyValue2.getDoubleValue()));
        final Map<String, Object> map = returnMap((String) actualValue.get(2));
        assertThat(((Number) map.get(OTelProtoCodec.REPLACE_DOT_WITH_AT.apply(childAttr1.getKey()))).longValue(), equalTo(childAttr1.getValue().getIntValue()));
        assertThat(map.get(OTelProtoCodec.REPLACE_DOT_WITH_AT.apply(childAttr2.getKey())), equalTo(childAttr2.getValue().getStringValue()));
        assertThat(((Number) map.get(OTelProtoCodec.REPLACE_DOT_WITH_AT.apply(childAttr1.getKey()))).longValue(), equalTo(childAttr1.getValue().getIntValue()));

    }


    @Test
    public void testInstrumentationLibraryAttributes() {
        final InstrumentationLibrary il1 = InstrumentationLibrary.newBuilder().setName("Jaeger").setVersion("0.6.0").build();
        final InstrumentationLibrary il2 = InstrumentationLibrary.newBuilder().setName("Jaeger").build();
        final InstrumentationLibrary il3 = InstrumentationLibrary.newBuilder().setVersion("0.6.0").build();
        final InstrumentationLibrary il4 = InstrumentationLibrary.newBuilder().build();

        assertThat(OTelProtoCodec.getInstrumentationLibraryAttributes(il1).size(), equalTo(2));
        assertThat(OTelProtoCodec.getInstrumentationLibraryAttributes(il1).get(OTelProtoCodec.INSTRUMENTATION_LIBRARY_NAME), equalTo(il1.getName()));
        assertThat(OTelProtoCodec.getInstrumentationLibraryAttributes(il1).get(OTelProtoCodec.INSTRUMENTATION_LIBRARY_VERSION), equalTo(il1.getVersion()));

        assertThat(OTelProtoCodec.getInstrumentationLibraryAttributes(il2).size(), equalTo(1));
        assertThat(OTelProtoCodec.getInstrumentationLibraryAttributes(il2).get(OTelProtoCodec.INSTRUMENTATION_LIBRARY_NAME), equalTo(il2.getName()));

        assertThat(OTelProtoCodec.getInstrumentationLibraryAttributes(il3).size(), equalTo(1));
        assertThat(OTelProtoCodec.getInstrumentationLibraryAttributes(il3).get(OTelProtoCodec.INSTRUMENTATION_LIBRARY_VERSION), equalTo(il3.getVersion()));

        assertThat(OTelProtoCodec.getInstrumentationLibraryAttributes(il4).isEmpty(), is(true));
    }

    @Test
    public void testStatusAttributes() {
        final Status st1 = Status.newBuilder().setCode(Status.StatusCode.STATUS_CODE_ERROR).setMessage("Some message").build();
        final Status st2 = Status.newBuilder().setMessage("error message").build();
        final Status st3 = Status.newBuilder().setCode(Status.StatusCode.STATUS_CODE_UNSET).build();
        final Status st4 = Status.newBuilder().build();

        assertThat(OTelProtoCodec.getSpanStatusAttributes(st1).size(), equalTo(2));
        assertThat(Status.StatusCode.forNumber((Integer) OTelProtoCodec.getSpanStatusAttributes(st1).get(OTelProtoCodec.STATUS_CODE)), equalTo(st1.getCode()));
        assertThat(OTelProtoCodec.getSpanStatusAttributes(st1).get(OTelProtoCodec.STATUS_MESSAGE), equalTo(st1.getMessage()));

        assertThat(OTelProtoCodec.getSpanStatusAttributes(st2).size(), equalTo(2));
        assertThat(Status.StatusCode.forNumber((Integer) OTelProtoCodec.getSpanStatusAttributes(st2).get(OTelProtoCodec.STATUS_CODE)), equalTo(st2.getCode()));

        assertThat(OTelProtoCodec.getSpanStatusAttributes(st3).size(), equalTo(1));
        assertThat(Status.StatusCode.forNumber((Integer) OTelProtoCodec.getSpanStatusAttributes(st3).get(OTelProtoCodec.STATUS_CODE)), equalTo(st3.getCode()));

        assertThat(OTelProtoCodec.getSpanStatusAttributes(st4).size(), equalTo(1));
        assertThat(Status.StatusCode.forNumber((Integer) OTelProtoCodec.getSpanStatusAttributes(st4).get(OTelProtoCodec.STATUS_CODE)), equalTo(st4.getCode()));

    }

    @Test
    public void testISO8601() {
        final long NANO_MULTIPLIER = 1_000 * 1_000 * 1_000;
        final io.opentelemetry.proto.trace.v1.Span startTimeUnixNano = io.opentelemetry.proto.trace.v1.Span.newBuilder()
                .setStartTimeUnixNano(651242400000000321L).build();
        final io.opentelemetry.proto.trace.v1.Span endTimeUnixNano = io.opentelemetry.proto.trace.v1.Span.newBuilder()
                .setEndTimeUnixNano(1598013600000000321L).build();
        final io.opentelemetry.proto.trace.v1.Span emptyTimeSpan = io.opentelemetry.proto.trace.v1.Span.newBuilder().build();

        final String startTime = OTelProtoCodec.getStartTimeISO8601(startTimeUnixNano);
        assertThat(Instant.parse(startTime).getEpochSecond() * NANO_MULTIPLIER + Instant.parse(startTime).getNano(), equalTo(startTimeUnixNano.getStartTimeUnixNano()));
        final String endTime = OTelProtoCodec.getEndTimeISO8601(endTimeUnixNano);
        assertThat(Instant.parse(endTime).getEpochSecond() * NANO_MULTIPLIER + Instant.parse(endTime).getNano(), equalTo(endTimeUnixNano.getEndTimeUnixNano()));
        final String emptyTime = OTelProtoCodec.getStartTimeISO8601(endTimeUnixNano);
        assertThat(Instant.parse(emptyTime).getEpochSecond() * NANO_MULTIPLIER + Instant.parse(emptyTime).getNano(), equalTo(emptyTimeSpan.getStartTimeUnixNano()));

    }

    @Test
    public void testTraceGroup() {
        final io.opentelemetry.proto.trace.v1.Span span1 = io.opentelemetry.proto.trace.v1.Span.newBuilder()
                .setParentSpanId(ByteString.copyFrom("PArentIdExists", StandardCharsets.UTF_8)).build();
        assertThat(OTelProtoCodec.getTraceGroup(span1), nullValue());
        final String testTraceGroup = "testTraceGroup";
        final io.opentelemetry.proto.trace.v1.Span span2 = io.opentelemetry.proto.trace.v1.Span.newBuilder()
                .setName(testTraceGroup).build();
        assertThat(OTelProtoCodec.getTraceGroup(span2), equalTo(testTraceGroup));
    }

    @Test
    public void testTraceGroupFields() {
        final io.opentelemetry.proto.trace.v1.Span span1 = io.opentelemetry.proto.trace.v1.Span.newBuilder()
                .setParentSpanId(ByteString.copyFrom("PArentIdExists", StandardCharsets.UTF_8)).build();
        final TraceGroupFields traceGroupFields1 = OTelProtoCodec.getTraceGroupFields(span1);
        assertThat(traceGroupFields1.getEndTime(), nullValue());
        assertThat(traceGroupFields1.getDurationInNanos(), nullValue());
        assertThat(traceGroupFields1.getStatusCode(), nullValue());
        final long testStartTimeUnixNano = 100;
        final long testEndTimeUnixNano = 100;
        final int testStatusCode = Status.StatusCode.STATUS_CODE_OK.getNumber();
        final io.opentelemetry.proto.trace.v1.Span span2 = io.opentelemetry.proto.trace.v1.Span.newBuilder()
                .setStartTimeUnixNano(testStartTimeUnixNano)
                .setEndTimeUnixNano(testEndTimeUnixNano)
                .setStatus(Status.newBuilder().setCodeValue(testStatusCode))
                .build();
        final TraceGroupFields expectedTraceGroupFields = DefaultTraceGroupFields.builder()
                .withStatusCode(testStatusCode)
                .withEndTime(OTelProtoCodec.getEndTimeISO8601(span2))
                .withDurationInNanos(testEndTimeUnixNano - testStartTimeUnixNano)
                .build();
        assertThat(OTelProtoCodec.getTraceGroupFields(span2), equalTo(expectedTraceGroupFields));
    }
}
