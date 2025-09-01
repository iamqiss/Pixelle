/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rest;

import org.density.common.annotation.ExperimentalApi;
import org.density.core.rest.RestStatus;
import org.density.http.HttpChunk;

import java.util.List;
import java.util.Map;

import org.reactivestreams.Publisher;

/**
 * A streaming channel used to prepare response and sending the response in chunks.
 *
 * @density.experimental
 */
@ExperimentalApi
public interface StreamingRestChannel extends RestChannel, Publisher<HttpChunk> {
    /**
     * Sends the next {@link HttpChunk} to the response stream
     * @param chunk response chunk
     */
    void sendChunk(HttpChunk chunk);

    /**
     * Prepares response before kicking of content streaming
     * @param status response status
     * @param headers response headers
     */
    void prepareResponse(RestStatus status, Map<String, List<String>> headers);

    /**
     * Returns {@code true} is this channel is ready for streaming request data, {@code false} otherwise
     * @return {@code true} is this channel is ready for streaming request data, {@code false} otherwise
     */
    boolean isReadable();

    /**
     * Returns {@code true} is this channel is ready for streaming response data, {@code false} otherwise
     * @return {@code true} is this channel is ready for streaming response data, {@code false} otherwise
     */
    boolean isWritable();
}
