/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.grpc.proto.response.common;

import org.density.DensityException;
import org.density.action.FailedNodeException;
import org.density.action.search.SearchPhaseExecutionException;
import org.density.action.search.ShardSearchFailure;
import org.density.common.breaker.ResponseLimitBreachedException;
import org.density.common.breaker.ResponseLimitSettings;
import org.density.core.common.ParsingException;
import org.density.core.common.breaker.CircuitBreakingException;
import org.density.protobufs.ErrorCause;
import org.density.protobufs.ObjectMap;
import org.density.protobufs.StringOrStringArray;
import org.density.script.ScriptException;
import org.density.search.SearchParseException;
import org.density.search.aggregations.MultiBucketConsumerService;
import org.density.test.DensityTestCase;
import org.density.transport.grpc.proto.response.exceptions.densityexception.DensityExceptionProtoUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DensityExceptionProtoUtilsTests extends DensityTestCase {
    private static final String TEST_NODE_ID = "test_node_id";

    public void testToProtoWithDensityException() throws IOException {
        // Create an DensityException
        DensityException exception = new DensityException("Test exception");

        // Convert to Protocol Buffer
        ErrorCause errorCause = DensityExceptionProtoUtils.toProto(exception);

        // Verify the conversion
        // The actual type format uses underscores instead of dots
        assertEquals("Should have the correct type", "exception", errorCause.getType());
        assertEquals("Should have the correct reason", "Test exception", errorCause.getReason());
        assertTrue("Should have a stack trace", errorCause.getStackTrace().length() > 0);
        assertFalse("Should not have suppressed exceptions", errorCause.getSuppressedList().iterator().hasNext());
        assertFalse("Should not have a cause", errorCause.hasCausedBy());
    }

    public void testToProtoWithNestedDensityException() throws IOException {
        // Create a nested DensityException
        IOException cause = new IOException("Cause exception");
        DensityException exception = new DensityException("Test exception", cause);

        // Convert to Protocol Buffer
        ErrorCause errorCause = DensityExceptionProtoUtils.toProto(exception);

        // Verify the conversion
        // The actual type format uses underscores instead of dots
        assertEquals("Should have the correct type", "exception", errorCause.getType());
        assertEquals("Should have the correct reason", "Test exception", errorCause.getReason());
        assertTrue("Should have a stack trace", errorCause.getStackTrace().length() > 0);

        // Verify the cause
        assertTrue("Should have a cause", errorCause.hasCausedBy());
        ErrorCause causedBy = errorCause.getCausedBy();
        // The actual type format uses underscores instead of dots
        assertEquals("Cause should have the correct type", "i_o_exception", causedBy.getType());
        assertEquals("Cause should have the correct reason", "Cause exception", causedBy.getReason());
    }

    public void testGenerateThrowableProtoWithDensityException() throws IOException {
        // Create an DensityException
        DensityException exception = new DensityException("Test exception");

        // Convert to Protocol Buffer
        ErrorCause errorCause = DensityExceptionProtoUtils.generateThrowableProto(exception);

        // Verify the conversion
        // The actual type format uses underscores instead of dots
        assertEquals("Should have the correct type", "exception", errorCause.getType());
        assertEquals("Should have the correct reason", "Test exception", errorCause.getReason());
    }

    public void testGenerateThrowableProtoWithIOException() throws IOException {
        // Create an IOException
        IOException exception = new IOException("Test IO exception");

        // Convert to Protocol Buffer
        ErrorCause errorCause = DensityExceptionProtoUtils.generateThrowableProto(exception);

        // Verify the conversion
        // The actual type format uses underscores instead of dots
        assertEquals("Should have the correct type", "i_o_exception", errorCause.getType());
        assertEquals("Should have the correct reason", "Test IO exception", errorCause.getReason());
    }

    public void testGenerateThrowableProtoWithRuntimeException() throws IOException {
        // Create a RuntimeException
        RuntimeException exception = new RuntimeException("Test runtime exception");

        // Convert to Protocol Buffer
        ErrorCause errorCause = DensityExceptionProtoUtils.generateThrowableProto(exception);

        // Verify the conversion
        // The actual type format uses underscores instead of dots
        assertEquals("Should have the correct type", "runtime_exception", errorCause.getType());
        assertEquals("Should have the correct reason", "Test runtime exception", errorCause.getReason());
    }

    public void testGenerateThrowableProtoWithNullMessage() throws IOException {
        // Create an exception with null message
        RuntimeException exception = new RuntimeException((String) null);

        // Convert to Protocol Buffer
        ErrorCause errorCause = DensityExceptionProtoUtils.generateThrowableProto(exception);

        // Verify the conversion
        // The actual type format uses underscores instead of dots
        assertEquals("Should have the correct type", "runtime_exception", errorCause.getType());
        assertFalse("Should not have a reason", errorCause.hasReason());
    }

    public void testGenerateThrowableProtoWithSuppressedExceptions() throws IOException {
        // Create an exception with suppressed exceptions
        RuntimeException exception = new RuntimeException("Main exception");
        exception.addSuppressed(new IllegalArgumentException("Suppressed exception"));

        // Convert to Protocol Buffer
        ErrorCause errorCause = DensityExceptionProtoUtils.generateThrowableProto(exception);

        // Verify the conversion
        // The actual type format uses underscores instead of dots
        assertEquals("Should have the correct type", "runtime_exception", errorCause.getType());
        assertEquals("Should have the correct reason", "Main exception", errorCause.getReason());

        // Verify suppressed exceptions
        assertEquals("Should have one suppressed exception", 1, errorCause.getSuppressedCount());
        ErrorCause suppressed = errorCause.getSuppressed(0);
        // The actual type format uses underscores instead of dots
        assertEquals("Suppressed should have the correct type", "illegal_argument_exception", suppressed.getType());
        assertEquals("Suppressed should have the correct reason", "Suppressed exception", suppressed.getReason());
    }

    public void testInnerToProtoWithBasicException() throws IOException {
        // Create a basic exception
        RuntimeException exception = new RuntimeException("Test exception");

        // Convert to Protocol Buffer
        ErrorCause errorCause = DensityExceptionProtoUtils.generateThrowableProto(exception);

        // Verify the conversion
        // The actual type format uses underscores instead of dots
        assertEquals("Should have the correct type", "runtime_exception", errorCause.getType());
        assertEquals("Should have the correct reason", "Test exception", errorCause.getReason());
        assertTrue("Should have a stack trace", errorCause.getStackTrace().length() > 0);
    }

    public void testHeaderToProtoWithSingleValue() throws IOException {
        // Create a header with a single value
        String key = "test-header";
        List<String> values = Collections.singletonList("test-value");

        // Convert to Protocol Buffer
        Map.Entry<String, StringOrStringArray> entry = DensityExceptionProtoUtils.headerToProto(key, values);

        // Verify the conversion
        assertNotNull("Entry should not be null", entry);
        assertEquals("Key should match", key, entry.getKey());
        assertTrue("Should be a string value", entry.getValue().hasStringValue());
        assertEquals("Value should match", "test-value", entry.getValue().getStringValue());
        assertFalse("Should not have a string array", entry.getValue().hasStringArray());
    }

    public void testHeaderToProtoWithMultipleValues() throws IOException {
        // Create a header with multiple values
        String key = "test-header";
        List<String> values = Arrays.asList("value1", "value2", "value3");

        // Convert to Protocol Buffer
        Map.Entry<String, StringOrStringArray> entry = DensityExceptionProtoUtils.headerToProto(key, values);

        // Verify the conversion
        assertNotNull("Entry should not be null", entry);
        assertEquals("Key should match", key, entry.getKey());
        assertFalse("Should not be a string value", entry.getValue().hasStringValue());
        assertTrue("Should have a string array", entry.getValue().hasStringArray());
        assertEquals("Array should have correct size", 3, entry.getValue().getStringArray().getStringArrayCount());
        assertEquals("First value should match", "value1", entry.getValue().getStringArray().getStringArray(0));
        assertEquals("Second value should match", "value2", entry.getValue().getStringArray().getStringArray(1));
        assertEquals("Third value should match", "value3", entry.getValue().getStringArray().getStringArray(2));
    }

    public void testHeaderToProtoWithEmptyValues() throws IOException {
        // Create a header with empty values
        String key = "test-header";
        List<String> values = Collections.emptyList();

        // Convert to Protocol Buffer
        Map.Entry<String, StringOrStringArray> entry = DensityExceptionProtoUtils.headerToProto(key, values);

        // Verify the conversion
        assertNull("Entry should be null for empty values", entry);
    }

    public void testHeaderToProtoWithNullValues() throws IOException {
        // Create a header with null values
        String key = "test-header";
        List<String> values = null;

        // Convert to Protocol Buffer
        Map.Entry<String, StringOrStringArray> entry = DensityExceptionProtoUtils.headerToProto(key, values);

        // Verify the conversion
        assertNull("Entry should be null for null values", entry);
    }

    public void testHeaderToValueProtoWithSingleValue() throws IOException {
        // Create a header with a single value
        String key = "test-header";
        List<String> values = Collections.singletonList("test-value");

        // Convert to Protocol Buffer
        Map.Entry<String, ObjectMap.Value> entry = DensityExceptionProtoUtils.headerToValueProto(key, values);

        // Verify the conversion
        assertNotNull("Entry should not be null", entry);
        assertEquals("Key should match", key, entry.getKey());
        assertTrue("Should be a string value", entry.getValue().hasString());
        assertEquals("Value should match", "test-value", entry.getValue().getString());
        assertFalse("Should not have a list value", entry.getValue().hasListValue());
    }

    public void testHeaderToValueProtoWithMultipleValues() throws IOException {
        // Create a header with multiple values
        String key = "test-header";
        List<String> values = Arrays.asList("value1", "value2", "value3");

        // Convert to Protocol Buffer
        Map.Entry<String, ObjectMap.Value> entry = DensityExceptionProtoUtils.headerToValueProto(key, values);

        // Verify the conversion
        assertNotNull("Entry should not be null", entry);
        assertEquals("Key should match", key, entry.getKey());
        assertFalse("Should not be a string value", entry.getValue().hasString());
        assertTrue("Should have a list value", entry.getValue().hasListValue());
        assertEquals("List should have correct size", 3, entry.getValue().getListValue().getValueCount());
        assertEquals("First value should match", "value1", entry.getValue().getListValue().getValue(0).getString());
        assertEquals("Second value should match", "value2", entry.getValue().getListValue().getValue(1).getString());
        assertEquals("Third value should match", "value3", entry.getValue().getListValue().getValue(2).getString());
    }

    public void testHeaderToValueProtoWithEmptyValues() throws IOException {
        // Create a header with empty values
        String key = "test-header";
        List<String> values = Collections.emptyList();

        // Convert to Protocol Buffer
        Map.Entry<String, ObjectMap.Value> entry = DensityExceptionProtoUtils.headerToValueProto(key, values);

        // Verify the conversion
        assertNull("Entry should be null for empty values", entry);
    }

    public void testHeaderToValueProtoWithNullValues() throws IOException {
        // Create a header with null values
        String key = "test-header";
        List<String> values = null;

        // Convert to Protocol Buffer
        Map.Entry<String, ObjectMap.Value> entry = DensityExceptionProtoUtils.headerToValueProto(key, values);

        // Verify the conversion
        assertNull("Entry should be null for null values", entry);
    }

    public void testMetadataToProtoWithCircuitBreakingException() {
        // Create a CircuitBreakingException with bytes wanted and bytes limit
        CircuitBreakingException exception = new CircuitBreakingException("Test circuit breaking", 1000L, 500L, null);

        // Convert to Protocol Buffer
        Map<String, ObjectMap.Value> metadata = DensityExceptionProtoUtils.metadataToProto(exception);

        // Verify the conversion
        assertNotNull("Metadata should not be null", metadata);
        assertTrue("Should have bytes_wanted field", metadata.containsKey("bytes_wanted"));
        assertEquals("bytes_wanted should match", 1000L, metadata.get("bytes_wanted").getInt64());
        assertTrue("Should have bytes_limit field", metadata.containsKey("bytes_limit"));
        assertEquals("bytes_limit should match", 500L, metadata.get("bytes_limit").getInt64());
        // Note: Durability is not in the constructor in newer versions
    }

    public void testMetadataToProtoWithFailedNodeException() {
        // Create a FailedNodeException
        FailedNodeException exception = new FailedNodeException(TEST_NODE_ID, "Test failed node", new IOException("IO error"));

        // Convert to Protocol Buffer
        Map<String, ObjectMap.Value> metadata = DensityExceptionProtoUtils.metadataToProto(exception);

        // Verify the conversion
        assertNotNull("Metadata should not be null", metadata);
        assertTrue("Should have node_id field", metadata.containsKey("node_id"));
        assertEquals("node_id should match", TEST_NODE_ID, metadata.get("node_id").getString());
    }

    public void testMetadataToProtoWithParsingException() {
        // Create a ParsingException with line and column numbers
        // Using a mock since we can't directly set line and column numbers
        ParsingException exception = mock(ParsingException.class);
        when(exception.getMessage()).thenReturn("Test parsing exception");
        when(exception.getLineNumber()).thenReturn(10);
        when(exception.getColumnNumber()).thenReturn(20);

        // Convert to Protocol Buffer
        Map<String, ObjectMap.Value> metadata = DensityExceptionProtoUtils.metadataToProto(exception);

        // Verify the conversion
        assertNotNull("Metadata should not be null", metadata);
        assertTrue("Should have line field", metadata.containsKey("line"));
        assertEquals("line should match", 10, metadata.get("line").getInt32());
        assertTrue("Should have col field", metadata.containsKey("col"));
        assertEquals("col should match", 20, metadata.get("col").getInt32());
    }

    public void testMetadataToProtoWithResponseLimitBreachedException() {
        // Create a ResponseLimitBreachedException
        ResponseLimitBreachedException exception = new ResponseLimitBreachedException(
            "Test response limit",
            1000,
            ResponseLimitSettings.LimitEntity.INDICES
        );

        // Convert to Protocol Buffer
        Map<String, ObjectMap.Value> metadata = DensityExceptionProtoUtils.metadataToProto(exception);

        // Verify the conversion
        assertNotNull("Metadata should not be null", metadata);
        assertTrue("Should have response_limit field", metadata.containsKey("response_limit"));
        assertEquals("response_limit should match", 1000L, metadata.get("response_limit").getInt32());
        assertTrue("Should have limit_entity field", metadata.containsKey("limit_entity"));
        assertEquals("limit_entity should match", "INDICES", metadata.get("limit_entity").getString());
    }

    public void testMetadataToProtoWithScriptException() {
        // Create a ScriptException
        ScriptException exception = new ScriptException(
            "Test script exception",
            new Exception("Script error"),
            Arrays.asList("line1", "line2"),
            "test_script",
            "painless"
        );

        // Convert to Protocol Buffer
        Map<String, ObjectMap.Value> metadata = DensityExceptionProtoUtils.metadataToProto(exception);

        // Verify the conversion
        assertNotNull("Metadata should not be null", metadata);
        assertTrue("Should have script_stack field", metadata.containsKey("script_stack"));
        assertTrue("Should have script field", metadata.containsKey("script"));
        assertEquals("script should match", "test_script", metadata.get("script").getString());
        assertTrue("Should have lang field", metadata.containsKey("lang"));
        assertEquals("lang should match", "painless", metadata.get("lang").getString());
    }

    public void testMetadataToProtoWithSearchParseException() {
        // Create a SearchParseException with line and column numbers
        // Using a mock since we can't directly set line and column numbers
        SearchParseException exception = mock(SearchParseException.class);
        when(exception.getMessage()).thenReturn("Test search parse exception");
        when(exception.getLineNumber()).thenReturn(10);
        when(exception.getColumnNumber()).thenReturn(20);

        // Convert to Protocol Buffer
        Map<String, ObjectMap.Value> metadata = DensityExceptionProtoUtils.metadataToProto(exception);

        // Verify the conversion
        assertNotNull("Metadata should not be null", metadata);
        assertTrue("Should have line field", metadata.containsKey("line"));
        assertEquals("line should match", 10, metadata.get("line").getInt32());
        assertTrue("Should have col field", metadata.containsKey("col"));
        assertEquals("col should match", 20, metadata.get("col").getInt32());
    }

    public void testMetadataToProtoWithSearchPhaseExecutionException() {
        // Create a SearchPhaseExecutionException
        SearchPhaseExecutionException exception = new SearchPhaseExecutionException(
            "test_phase",
            "Test search phase execution",
            ShardSearchFailure.EMPTY_ARRAY
        );

        // Convert to Protocol Buffer
        Map<String, ObjectMap.Value> metadata = DensityExceptionProtoUtils.metadataToProto(exception);

        // Verify the conversion
        assertNotNull("Metadata should not be null", metadata);
        assertTrue("Should have phase field", metadata.containsKey("phase"));
        assertEquals("phase should match", "test_phase", metadata.get("phase").getString());
        assertTrue("Should have grouped field", metadata.containsKey("grouped"));
        assertTrue("grouped should be true", metadata.get("grouped").getBool());
    }

    public void testMetadataToProtoWithTooManyBucketsException() {
        // Create a TooManyBucketsException
        MultiBucketConsumerService.TooManyBucketsException exception = new MultiBucketConsumerService.TooManyBucketsException(
            "Test too many buckets",
            1000
        );

        // Convert to Protocol Buffer
        Map<String, ObjectMap.Value> metadata = DensityExceptionProtoUtils.metadataToProto(exception);

        // Verify the conversion
        assertNotNull("Metadata should not be null", metadata);
        assertTrue("Should have max_buckets field", metadata.containsKey("max_buckets"));
        assertEquals("max_buckets should match", 1000, metadata.get("max_buckets").getInt32());
    }

    public void testMetadataToProtoWithGenericDensityException() {
        // Create a generic DensityException
        DensityException exception = new DensityException("Test generic exception");

        // Convert to Protocol Buffer
        Map<String, ObjectMap.Value> metadata = DensityExceptionProtoUtils.metadataToProto(exception);

        // Verify the conversion
        assertNotNull("Metadata should not be null", metadata);
        assertTrue("Metadata should be empty for generic exception", metadata.isEmpty());
    }
}
