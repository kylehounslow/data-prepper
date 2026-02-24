/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.oteltrace;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.DefaultTraceGroupFields;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GenAiEnrichmentTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private OTelTraceRawProcessor processor;

    @BeforeEach
    void setup() {
        final OtelTraceRawProcessorConfig config = mock(OtelTraceRawProcessorConfig.class);
        when(config.getTraceFlushIntervalSeconds()).thenReturn(180L);
        when(config.getTraceGroupCacheMaxSize()).thenReturn(OtelTraceRawProcessorConfig.MAX_TRACE_ID_CACHE_SIZE);
        when(config.getTraceGroupCacheTimeToLive()).thenReturn(OtelTraceRawProcessorConfig.DEFAULT_TRACE_ID_TTL);

        final PipelineDescription pipelineDescription = mock(PipelineDescription.class);

        processor = new OTelTraceRawProcessor(config, pipelineDescription, mock(PluginMetrics.class));
    }

    @Test
    void testGenAiSystemPropagatedFromChildToRoot() {
        final Collection<Record<Span>> result = processor.doExecute(storageFormatRecords(
                "genai-root-span.json", "genai-child-span.json"));

        final Span root = findRoot(result);
        assertEquals("aws.bedrock", root.getAttributes().get("span.attributes.gen_ai@system"));
    }

    @Test
    void testRootWithExistingGenAiNotOverwritten() {
        final Collection<Record<Span>> result = processor.doExecute(storageFormatRecords(
                "genai-root-span-with-genai-attr.json", "genai-child-span-with-genai-root.json"));

        final Span root = findRoot(result);
        assertEquals("openai", root.getAttributes().get("span.attributes.gen_ai@system"));
    }

    @Test
    void testTokenAggregation() {
        final Collection<Record<Span>> result = processor.doExecute(storageFormatRecords(
                "genai-tokens-root-span.json", "genai-tokens-child-span-1.json", "genai-tokens-child-span-2.json"));

        final Span root = findRoot(result);
        assertEquals(300L, ((Number) root.getAttributes().get("span.attributes.gen_ai@usage@input_tokens")).longValue());
        assertEquals(125L, ((Number) root.getAttributes().get("span.attributes.gen_ai@usage@output_tokens")).longValue());
    }

    @Test
    void testTokenAggregationSkippedWhenRootHasTokens() {
        final Collection<Record<Span>> result = processor.doExecute(storageFormatRecords(
                "genai-tokens-root-span-with-tokens.json", "genai-tokens-child-span-with-root-tokens.json"));

        final Span root = findRoot(result);
        assertEquals(500, ((Number) root.getAttributes().get("span.attributes.gen_ai@usage@input_tokens")).intValue());
        assertEquals(200, ((Number) root.getAttributes().get("span.attributes.gen_ai@usage@output_tokens")).intValue());
    }

    @Test
    void testFlattenedSubkeysStripped() {
        final Collection<Record<Span>> result = processor.doExecute(storageFormatRecords(
                "genai-flattened-subkeys-root-span.json", "genai-flattened-subkeys-child-span.json"));

        for (final Record<Span> record : result) {
            final Map<String, Object> attrs = record.getData().getAttributes();
            for (final String key : attrs.keySet()) {
                assertFalse(key.matches("span\\.attributes\\.llm@input_messages@\\d+.*"),
                        "Flattened sub-key should be stripped: " + key);
            }
        }
    }

    @Test
    void testNoEnrichmentForNonGenAiTraces() {
        final Collection<Record<Span>> result = processor.doExecute(storageFormatRecords(
                "plain-root-span.json", "plain-child-span.json"));

        final Span root = findRoot(result);
        assertNull(root.getAttributes().get("span.attributes.gen_ai@system"));
    }

    @Test
    void testOpenInferenceNormalization() {
        final Collection<Record<Span>> result = processor.doExecute(storageFormatRecords(
                "genai-root-span.json", "openinference-child-span.json"));

        final Span child = result.stream().map(Record::getData)
                .filter(s -> s.getParentSpanId() != null && !s.getParentSpanId().isEmpty())
                .findFirst().orElseThrow();
        final Map<String, Object> attrs = child.getAttributes();

        // Normalized attributes should exist in storage format
        assertEquals(150, ((Number) attrs.get("span.attributes.gen_ai@usage@input_tokens")).intValue());
        assertEquals(42, ((Number) attrs.get("span.attributes.gen_ai@usage@output_tokens")).intValue());
        assertEquals("claude-3-sonnet", attrs.get("span.attributes.gen_ai@request@model"));
        assertEquals("anthropic", attrs.get("span.attributes.gen_ai@provider@name"));
        assertEquals("chat", attrs.get("span.attributes.gen_ai@operation@name")); // LLM -> chat
        assertEquals("weather-agent", attrs.get("span.attributes.gen_ai@agent@name"));
        assertEquals("sess-123", attrs.get("span.attributes.gen_ai@conversation@id"));

        // Originals should be preserved in storage format
        assertEquals(150, ((Number) attrs.get("span.attributes.llm@token_count@prompt")).intValue());
        assertEquals("anthropic", attrs.get("span.attributes.llm@provider"));
    }

    @Test
    void testOpenLLMetryNormalization() {
        final Collection<Record<Span>> result = processor.doExecute(storageFormatRecords(
                "genai-root-span.json", "openllmetry-child-span.json"));

        final Span child = result.stream().map(Record::getData)
                .filter(s -> s.getParentSpanId() != null && !s.getParentSpanId().isEmpty())
                .findFirst().orElseThrow();
        final Map<String, Object> attrs = child.getAttributes();

        assertEquals(200, ((Number) attrs.get("span.attributes.gen_ai@usage@input_tokens")).intValue());
        assertEquals(55, ((Number) attrs.get("span.attributes.gen_ai@usage@output_tokens")).intValue());
        assertEquals("gpt-4o", attrs.get("span.attributes.gen_ai@request@model"));
        assertEquals("chat", attrs.get("span.attributes.gen_ai@operation@name")); // chat -> chat
        assertEquals("[\"stop\"]", attrs.get("span.attributes.gen_ai@response@finish_reasons")); // wrapSlice
        assertEquals("research-agent", attrs.get("span.attributes.gen_ai@agent@name"));

        // Originals preserved in storage format
        assertEquals(200, ((Number) attrs.get("span.attributes.llm@usage@prompt_tokens")).intValue());
    }

    @Test
    void testNormalizationSkipsExistingTarget() {
        final Collection<Record<Span>> result = processor.doExecute(storageFormatRecords(
                "genai-root-span.json", "genai-child-span.json"));

        // genai-child-span already has gen_ai.system = aws.bedrock (in storage format after conversion)
        final Span child = result.stream().map(Record::getData)
                .filter(s -> s.getParentSpanId() != null && !s.getParentSpanId().isEmpty())
                .findFirst().orElseThrow();
        assertEquals("aws.bedrock", child.getAttributes().get("span.attributes.gen_ai@system"));
    }

    @Test
    void testNormalizationEnablesTokenAggregation() {
        // OpenInference child has llm.token_count.prompt/completion, not gen_ai.usage.*
        // After normalization, tokens should be aggregated to root
        final Collection<Record<Span>> result = processor.doExecute(storageFormatRecords(
                "genai-root-span.json", "openinference-child-span.json"));

        final Span root = findRoot(result);
        assertEquals(150L, ((Number) root.getAttributes().get("span.attributes.gen_ai@usage@input_tokens")).longValue());
        assertEquals(42L, ((Number) root.getAttributes().get("span.attributes.gen_ai@usage@output_tokens")).longValue());
    }

    /**
     * Builds records from JSON files and converts attribute keys to storage format,
     * simulating what OTelProtoOpensearchCodec does when parsing real OTLP spans.
     * Dot-notation keys (e.g. "gen_ai.system") become "span.attributes.gen_ai@system".
     */
    private List<Record<Span>> storageFormatRecords(final String... files) {
        return Stream.of(files)
                .map(GenAiEnrichmentTest::buildSpanFromJsonFile)
                .map(GenAiEnrichmentTest::convertToStorageFormat)
                .map(Record::new)
                .collect(Collectors.toList());
    }

    /**
     * Converts a span's attribute keys from dot-notation to the storage format used by
     * OTelProtoOpensearchCodec: "gen_ai.system" → "span.attributes.gen_ai@system".
     */
    private static Span convertToStorageFormat(final Span span) {
        final Map<String, Object> attrs = span.getAttributes();
        if (attrs == null || attrs.isEmpty()) {
            return span;
        }
        final Map<String, Object> converted = new java.util.LinkedHashMap<>();
        for (final Map.Entry<String, Object> entry : attrs.entrySet()) {
            converted.put("span.attributes." + entry.getKey().replace('.', '@'), entry.getValue());
        }
        return JacksonSpan.builder()
                .withTraceId(span.getTraceId())
                .withSpanId(span.getSpanId())
                .withParentSpanId(span.getParentSpanId())
                .withTraceState(span.getTraceState())
                .withName(span.getName())
                .withKind(span.getKind())
                .withDurationInNanos(span.getDurationInNanos())
                .withStartTime(span.getStartTime())
                .withEndTime(span.getEndTime())
                .withTraceGroup(span.getTraceGroup())
                .withTraceGroupFields(span.getTraceGroupFields())
                .withAttributes(converted)
                .build();
    }

    private List<Record<Span>> records(final String... files) {
        return Stream.of(files)
                .map(GenAiEnrichmentTest::buildSpanFromJsonFile)
                .map(Record::new)
                .collect(Collectors.toList());
    }

    private Span findRoot(final Collection<Record<Span>> records) {
        return records.stream()
                .map(Record::getData)
                .filter(s -> s.getParentSpanId() == null || s.getParentSpanId().isEmpty())
                .findFirst()
                .orElseThrow(() -> new AssertionError("No root span found"));
    }

    private static Span buildSpanFromJsonFile(final String jsonFileName) {
        JacksonSpan.Builder spanBuilder = JacksonSpan.builder();
        try (final InputStream inputStream = Objects.requireNonNull(
                GenAiEnrichmentTest.class.getClassLoader().getResourceAsStream(jsonFileName))) {
            final Map<String, Object> spanMap = OBJECT_MAPPER.readValue(inputStream, new TypeReference<Map<String, Object>>() {});
            spanBuilder = spanBuilder
                    .withTraceId((String) spanMap.get("traceId"))
                    .withSpanId((String) spanMap.get("spanId"))
                    .withParentSpanId((String) spanMap.get("parentSpanId"))
                    .withTraceState((String) spanMap.get("traceState"))
                    .withName((String) spanMap.get("name"))
                    .withKind((String) spanMap.get("kind"))
                    .withDurationInNanos(((Number) spanMap.get("durationInNanos")).longValue())
                    .withStartTime((String) spanMap.get("startTime"))
                    .withEndTime((String) spanMap.get("endTime"))
                    .withTraceGroup(null);
            final Map<String, Object> status = (Map<String, Object>) spanMap.get("status");
            if (status != null) {
                spanBuilder = spanBuilder.withStatus(status);
            }
            final Map<String, Object> attributes = (Map<String, Object>) spanMap.get("attributes");
            if (attributes != null) {
                spanBuilder = spanBuilder.withAttributes(attributes);
            }
            DefaultTraceGroupFields.Builder tgBuilder = DefaultTraceGroupFields.builder();
            final String parentSpanId = (String) spanMap.get("parentSpanId");
            if (parentSpanId == null || parentSpanId.isEmpty()) {
                final Map<String, Object> tgFields = (Map<String, Object>) spanMap.get("traceGroupFields");
                if (tgFields != null) {
                    tgBuilder = tgBuilder
                            .withStatusCode((Integer) tgFields.get("statusCode"))
                            .withEndTime((String) spanMap.get("endTime"))
                            .withDurationInNanos(((Number) spanMap.get("durationInNanos")).longValue());
                }
                spanBuilder = spanBuilder.withTraceGroup((String) spanMap.get("traceGroup"));
            }
            spanBuilder.withTraceGroupFields(tgBuilder.build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return spanBuilder.build();
    }
}
