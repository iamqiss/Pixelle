/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport;

/**
 * Base class TcpTransportChannel
 */
public abstract class BaseTcpTransportChannel implements TransportChannel {
    private final TcpChannel channel;

    /**
     * Constructor.
     * @param channel tcp channel
     */
    public BaseTcpTransportChannel(TcpChannel channel) {
        this.channel = channel;
    }

    /**
     * Returns {@link TcpChannel}
     * @return TcpChannel
     */
    public TcpChannel getChannel() {
        return channel;
    }

}
