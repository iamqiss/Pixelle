/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport;

import org.density.Version;
import org.density.cluster.node.DiscoveryNode;
import org.density.core.transport.TransportResponse;

import java.io.IOException;
import java.util.Set;

/**
 * Protocol based outbound data handler.
 * Different transport protocols can have different implementations of this class.
 *
 * @density.internal
 */
public abstract class ProtocolOutboundHandler {

    /**
     * Sends the request to the given channel. This method should be used to send {@link TransportRequest}
     * objects back to the caller.
     */
    public abstract void sendRequest(
        final DiscoveryNode node,
        final TcpChannel channel,
        final long requestId,
        final String action,
        final TransportRequest request,
        final TransportRequestOptions options,
        final Version channelVersion,
        final boolean compressRequest,
        final boolean isHandshake
    ) throws IOException, TransportException;

    /**
     * Sends the response to the given channel. This method should be used to send {@link TransportResponse}
     * objects back to the caller.
     *
     * @see #sendErrorResponse(Version, Set, TcpChannel, long, String, Exception) for sending error responses
     */
    public abstract void sendResponse(
        final Version nodeVersion,
        final Set<String> features,
        final TcpChannel channel,
        final long requestId,
        final String action,
        final TransportResponse response,
        final boolean compress,
        final boolean isHandshake
    ) throws IOException;

    /**
     * Sends back an error response to the caller via the given channel
     */
    public abstract void sendErrorResponse(
        final Version nodeVersion,
        final Set<String> features,
        final TcpChannel channel,
        final long requestId,
        final String action,
        final Exception error
    ) throws IOException;

    protected abstract void setMessageListener(TransportMessageListener listener);
}
