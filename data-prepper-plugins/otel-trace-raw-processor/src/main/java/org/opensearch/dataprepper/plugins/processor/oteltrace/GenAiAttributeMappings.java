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

import java.util.HashMap;
import java.util.Map;

/**
 * Attribute mappings from vendor-specific instrumentation libraries to OTel GenAI Semantic Conventions v1.39.0.
 * Ported from <a href="https://github.com/kylehounslow/genainormalizer">genainormalizer</a>.
 */
final class GenAiAttributeMappings {

    static final class MappingTarget {
        private final String key;
        private final boolean wrapSlice;

        MappingTarget(final String key) {
            this(key, false);
        }

        MappingTarget(final String key, final boolean wrapSlice) {
            this.key = key;
            this.wrapSlice = wrapSlice;
        }

        String getKey() {
            return key;
        }

        boolean isWrapSlice() {
            return wrapSlice;
        }
    }

    private GenAiAttributeMappings() {}

    private static final Map<String, MappingTarget> LOOKUP_TABLE = buildLookupTable();
    private static final Map<String, String> OPERATION_NAME_VALUES = buildOperationNameValues();

    /** Combined lookup table for all profiles. */
    static Map<String, MappingTarget> getLookupTable() {
        return LOOKUP_TABLE;
    }

    /** Value mappings for gen_ai.operation.name (case-insensitive). */
    static Map<String, String> getOperationNameValues() {
        return OPERATION_NAME_VALUES;
    }

    private static Map<String, MappingTarget> buildLookupTable() {
        final Map<String, MappingTarget> table = new HashMap<>();

        // --- OpenInference (Arize) ---
        // https://github.com/Arize-ai/openinference/blob/main/spec/semantic_conventions.md
        table.put("llm.token_count.prompt", new MappingTarget("gen_ai.usage.input_tokens"));
        table.put("llm.token_count.completion", new MappingTarget("gen_ai.usage.output_tokens"));
        table.put("llm.model_name", new MappingTarget("gen_ai.request.model"));
        table.put("llm.provider", new MappingTarget("gen_ai.provider.name"));
        table.put("llm.input_messages", new MappingTarget("gen_ai.input.messages"));
        table.put("llm.output_messages", new MappingTarget("gen_ai.output.messages"));
        table.put("embedding.model_name", new MappingTarget("gen_ai.request.model"));
        table.put("tool.name", new MappingTarget("gen_ai.tool.name"));
        table.put("tool.description", new MappingTarget("gen_ai.tool.description"));
        table.put("tool_call.function.arguments", new MappingTarget("gen_ai.tool.call.arguments"));
        table.put("tool_call.id", new MappingTarget("gen_ai.tool.call.id"));
        table.put("reranker.model_name", new MappingTarget("gen_ai.request.model"));
        table.put("agent.name", new MappingTarget("gen_ai.agent.name"));
        table.put("session.id", new MappingTarget("gen_ai.conversation.id"));
        table.put("openinference.span.kind", new MappingTarget("gen_ai.operation.name"));

        // --- OpenLLMetry (Traceloop) ---
        // https://www.traceloop.com/docs/openllmetry/contributing/semantic-conventions
        table.put("llm.usage.prompt_tokens", new MappingTarget("gen_ai.usage.input_tokens"));
        table.put("llm.usage.completion_tokens", new MappingTarget("gen_ai.usage.output_tokens"));
        table.put("llm.request.model", new MappingTarget("gen_ai.request.model"));
        table.put("llm.response.model", new MappingTarget("gen_ai.response.model"));
        table.put("llm.request.max_tokens", new MappingTarget("gen_ai.request.max_tokens"));
        table.put("llm.request.temperature", new MappingTarget("gen_ai.request.temperature"));
        table.put("llm.request.top_p", new MappingTarget("gen_ai.request.top_p"));
        table.put("llm.top_k", new MappingTarget("gen_ai.request.top_k"));
        table.put("llm.frequency_penalty", new MappingTarget("gen_ai.request.frequency_penalty"));
        table.put("llm.presence_penalty", new MappingTarget("gen_ai.request.presence_penalty"));
        table.put("llm.chat.stop_sequences", new MappingTarget("gen_ai.request.stop_sequences"));
        table.put("llm.request.functions", new MappingTarget("gen_ai.tool.definitions"));
        table.put("llm.response.finish_reason", new MappingTarget("gen_ai.response.finish_reasons", true));
        table.put("llm.response.stop_reason", new MappingTarget("gen_ai.response.finish_reasons", true));
        table.put("llm.request.type", new MappingTarget("gen_ai.operation.name"));
        table.put("traceloop.span.kind", new MappingTarget("gen_ai.operation.name"));
        table.put("traceloop.entity.name", new MappingTarget("gen_ai.agent.name"));
        table.put("traceloop.entity.input", new MappingTarget("gen_ai.input.messages"));
        table.put("traceloop.entity.output", new MappingTarget("gen_ai.output.messages"));

        return table;
    }

    private static Map<String, String> buildOperationNameValues() {
        final Map<String, String> raw = new HashMap<>();
        // OpenInference span kinds
        raw.put("LLM", "chat");
        raw.put("EMBEDDING", "embeddings");
        raw.put("CHAIN", "invoke_agent");
        raw.put("RETRIEVER", "retrieval");
        raw.put("RERANKER", "retrieval");
        raw.put("TOOL", "execute_tool");
        raw.put("AGENT", "invoke_agent");
        raw.put("PROMPT", "text_completion");
        // OpenLLMetry traceloop.span.kind
        raw.put("workflow", "invoke_agent");
        raw.put("task", "invoke_agent");
        raw.put("agent", "invoke_agent");
        raw.put("tool", "execute_tool");
        // OpenLLMetry llm.request.type
        raw.put("completion", "text_completion");
        raw.put("chat", "chat");
        raw.put("rerank", "retrieval");
        raw.put("embedding", "embeddings");

        // Normalize keys to lowercase for case-insensitive lookup
        final Map<String, String> normalized = new HashMap<>();
        for (final Map.Entry<String, String> entry : raw.entrySet()) {
            normalized.put(entry.getKey().toLowerCase(), entry.getValue());
        }
        return normalized;
    }
}
