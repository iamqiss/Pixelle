/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport;

import org.density.common.annotation.PublicApi;

/**
 * Base class for inbound data as a message.
 * Different implementations are used for different protocols.
 *
 * @density.internal
 */
@PublicApi(since = "2.14.0")
public interface ProtocolInboundMessage {

    /**
     * @return the protocol used to encode this message
     */
    public String getProtocol();

}
