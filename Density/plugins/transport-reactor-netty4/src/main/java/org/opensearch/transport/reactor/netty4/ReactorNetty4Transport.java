/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.reactor.netty4;

import org.density.common.settings.Setting;
import org.density.common.settings.Setting.Property;
import org.density.common.util.concurrent.DensityExecutors;

import reactor.netty.tcp.TcpServer;

/**
 * The transport implementations based on Reactor Netty (see please {@link TcpServer}).
 */
public class ReactorNetty4Transport {
    /**
     * The number of Netty workers
     */
    public static final Setting<Integer> SETTING_WORKER_COUNT = new Setting<>(
        "transport.netty.worker_count",
        (s) -> Integer.toString(DensityExecutors.allocatedProcessors(s)),
        (s) -> Setting.parseInt(s, 1, "transport.netty.worker_count"),
        Property.NodeScope
    );

    /**
     * Default constructor
     */
    public ReactorNetty4Transport() {}
}
