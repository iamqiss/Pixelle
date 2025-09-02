/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.grpc.proto.response.exceptions.densityexception;

import org.density.ExceptionsHelper;
import org.density.DensityException;
import org.density.action.FailedNodeException;
import org.density.action.search.SearchPhaseExecutionException;
import org.density.common.breaker.ResponseLimitBreachedException;
import org.density.core.common.ParsingException;
import org.density.core.common.breaker.CircuitBreakingException;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.protobufs.ErrorCause;
import org.density.protobufs.ObjectMap;
import org.density.protobufs.StringArray;
import org.density.protobufs.StringOrStringArray;
import org.density.script.ScriptException;
import org.density.search.SearchParseException;
import org.density.search.aggregations.MultiBucketConsumerService;
import org.density.transport.grpc.proto.response.exceptions.CircuitBreakingExceptionProtoUtils;
import org.density.transport.grpc.proto.response.exceptions.FailedNodeExceptionProtoUtils;
import org.density.transport.grpc.proto.response.exceptions.ParsingExceptionProtoUtils;
import org.density.transport.grpc.proto.response.exceptions.ResponseLimitBreachedExceptionProtoUtils;
import org.density.transport.grpc.proto.response.exceptions.ScriptExceptionProtoUtils;
import org.density.transport.grpc.proto.response.exceptions.SearchParseExceptionProtoUtils;
import org.density.transport.grpc.proto.response.exceptions.SearchPhaseExecutionExceptionProtoUtils;
import org.density.transport.grpc.proto.response.exceptions.TooManyBucketsExceptionProtoUtils;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.density.DensityException.DENSITY_PREFIX_KEY;
import static org.density.DensityException.getExceptionName;

/**
 * Utility class for converting DensityException objects to Protocol Buffers.
 */
public class DensityExceptionProtoUtils {

    private DensityExceptionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts an DensityException to its Protocol Buffer representation.
     * This method is equivalent to the {@link DensityException#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param exception The DensityException to convert
     * @return A Protocol Buffer ErrorCause representation
     * @throws IOException if there's an error during conversion
     */
    public static ErrorCause toProto(DensityException exception) throws IOException {
        Throwable ex = ExceptionsHelper.unwrapCause(exception);
        if (ex != exception) {
            return generateThrowableProto(ex);
        } else {
            return innerToProto(
                exception,
                getExceptionName(exception),
                exception.getMessage(),
                exception.getHeaders(),
                exception.getMetadata(),
                exception.getCause()
            );
        }
    }

    /**
     * Static helper method that renders {@link DensityException} or {@link Throwable} instances
     * as Protocol Buffers.
     * <p>
     * This method is usually used when the {@link Throwable} is rendered as a part of another Protocol Buffer object.
     * It is equivalent to the {@link DensityException#generateThrowableXContent(XContentBuilder, ToXContent.Params, Throwable)}
     *
     * @param t The throwable to convert
     * @return A Protocol Buffer ErrorCause representation
     * @throws IOException if there's an error during conversion
     */
    public static ErrorCause generateThrowableProto(Throwable t) throws IOException {
        t = ExceptionsHelper.unwrapCause(t);

        if (t instanceof DensityException) {
            return toProto((DensityException) t);
        } else {
            return innerToProto(t, getExceptionName(t), t.getMessage(), emptyMap(), emptyMap(), t.getCause());
        }
    }

    /**
     * Inner helper method for converting a Throwable to its Protocol Buffer representation.
     * This method is equivalent to the {@link DensityException#innerToXContent(XContentBuilder, ToXContent.Params, Throwable, String, String, Map, Map, Throwable)}.
     *
     * @param throwable The throwable to convert
     * @param type The exception type
     * @param message The exception message
     * @param headers The exception headers
     * @param metadata The exception metadata
     * @param cause The exception cause
     * @return A Protocol Buffer ErrorCause representation
     * @throws IOException if there's an error during conversion
     */
    public static ErrorCause innerToProto(
        Throwable throwable,
        String type,
        String message,
        Map<String, List<String>> headers,
        Map<String, List<String>> metadata,
        Throwable cause
    ) throws IOException {
        ErrorCause.Builder errorCauseBuilder = ErrorCause.newBuilder();

        // Set exception type
        errorCauseBuilder.setType(type);

        // Set exception message (reason)
        if (message != null) {
            errorCauseBuilder.setReason(message);
        }

        // Add custom metadata fields propogated by the child classes of DensityException
        for (Map.Entry<String, List<String>> entry : metadata.entrySet()) {
            Map.Entry<String, ObjectMap.Value> protoEntry = headerToValueProto(
                entry.getKey().substring(DENSITY_PREFIX_KEY.length()),
                entry.getValue()
            );
            errorCauseBuilder.putMetadata(protoEntry.getKey(), protoEntry.getValue());
        }

        // Add metadata if the throwable is an DensityException
        if (throwable instanceof DensityException) {
            DensityException exception = (DensityException) throwable;
            Map<String, ObjectMap.Value> moreMetadata = metadataToProto(exception);
            for (Map.Entry<String, ObjectMap.Value> entry : moreMetadata.entrySet()) {
                errorCauseBuilder.putMetadata(entry.getKey(), entry.getValue());
            }
        }

        if (cause != null) {
            errorCauseBuilder.setCausedBy(generateThrowableProto(cause));
        }

        if (headers.isEmpty() == false) {
            for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
                Map.Entry<String, StringOrStringArray> protoEntry = headerToProto(entry.getKey(), entry.getValue());
                errorCauseBuilder.putHeader(protoEntry.getKey(), protoEntry.getValue());
            }
        }

        // Add stack trace
        errorCauseBuilder.setStackTrace(ExceptionsHelper.stackTrace(throwable));

        // Add suppressed exceptions
        Throwable[] allSuppressed = throwable.getSuppressed();
        if (allSuppressed.length > 0) {
            for (Throwable suppressed : allSuppressed) {
                errorCauseBuilder.addSuppressed(generateThrowableProto(suppressed));
            }
        }

        return errorCauseBuilder.build();
    }

    /**
     * Converts a single entry of a {@code Map<String, List<String>>} into a protobuf {@code <String, StringOrStringArray> }
     * Similar to {@link DensityException#headerToXContent(XContentBuilder, String, List)}
     *
     * @param key The key of the header entry
     * @param values The list of values for the header entry
     * @return A map entry containing the key and its corresponding StringOrStringArray value, or null if values is null or empty
     * @throws IOException if there's an error during conversion
     */
    public static Map.Entry<String, StringOrStringArray> headerToProto(String key, List<String> values) throws IOException {
        if (values != null && values.isEmpty() == false) {
            if (values.size() == 1) {
                return new AbstractMap.SimpleEntry<String, StringOrStringArray>(
                    key,
                    StringOrStringArray.newBuilder().setStringValue(values.get(0)).build()
                );
            } else {
                StringArray.Builder stringArrayBuilder = StringArray.newBuilder();
                for (String val : values) {
                    stringArrayBuilder.addStringArray(val);
                }
                StringOrStringArray stringOrStringArray = StringOrStringArray.newBuilder()
                    .setStringArray(stringArrayBuilder.build())
                    .build();

                return new AbstractMap.SimpleEntry<String, StringOrStringArray>(key, stringOrStringArray);
            }
        }
        return null;
    }

    /**
     * Similar to {@link DensityExceptionProtoUtils#headerToProto(String, List)},
     * but returns a {@code Map<String, ObjectMap.Value>} instead.
     *
     * @param key The key of the header entry
     * @param values The list of values for the header entry
     * @return A map entry containing the key and its corresponding ObjectMap.Value, or null if values is null or empty
     * @throws IOException if there's an error during conversion
     */
    public static Map.Entry<String, ObjectMap.Value> headerToValueProto(String key, List<String> values) throws IOException {
        if (values != null && values.isEmpty() == false) {
            if (values.size() == 1) {
                return new AbstractMap.SimpleEntry<String, ObjectMap.Value>(
                    key,
                    ObjectMap.Value.newBuilder().setString(values.get(0)).build()
                );
            } else {
                ObjectMap.ListValue.Builder listValueBuilder = ObjectMap.ListValue.newBuilder();
                for (String val : values) {
                    listValueBuilder.addValue(ObjectMap.Value.newBuilder().setString(val).build());
                }
                return new AbstractMap.SimpleEntry<String, ObjectMap.Value>(
                    key,
                    ObjectMap.Value.newBuilder().setListValue(listValueBuilder).build()
                );
            }
        }
        return null;
    }

    /**
     * This method is similar to {@link DensityException#metadataToXContent(XContentBuilder, ToXContent.Params)}
     * This method is overridden by various exception classes, which are hardcoded here.
     *
     * @param exception The DensityException to convert metadata from
     * @return A map containing the exception's metadata as ObjectMap.Value objects
     */
    public static Map<String, ObjectMap.Value> metadataToProto(DensityException exception) {
        if (exception instanceof CircuitBreakingException) {
            return CircuitBreakingExceptionProtoUtils.metadataToProto((CircuitBreakingException) exception);
        } else if (exception instanceof FailedNodeException) {
            return FailedNodeExceptionProtoUtils.metadataToProto((FailedNodeException) exception);
        } else if (exception instanceof ParsingException) {
            return ParsingExceptionProtoUtils.metadataToProto((ParsingException) exception);
        } else if (exception instanceof ResponseLimitBreachedException) {
            return ResponseLimitBreachedExceptionProtoUtils.metadataToProto((ResponseLimitBreachedException) exception);
        } else if (exception instanceof ScriptException) {
            return ScriptExceptionProtoUtils.metadataToProto((ScriptException) exception);
        } else if (exception instanceof SearchParseException) {
            return SearchParseExceptionProtoUtils.metadataToProto((SearchParseException) exception);
        } else if (exception instanceof SearchPhaseExecutionException) {
            return SearchPhaseExecutionExceptionProtoUtils.metadataToProto((SearchPhaseExecutionException) exception);
        } else if (exception instanceof MultiBucketConsumerService.TooManyBucketsException) {
            return TooManyBucketsExceptionProtoUtils.metadataToProto((MultiBucketConsumerService.TooManyBucketsException) exception);
        } else {
            return new HashMap<>();
        }
    }
}
