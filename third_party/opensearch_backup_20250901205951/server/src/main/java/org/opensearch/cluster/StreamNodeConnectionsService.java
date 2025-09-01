/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster;

import org.density.common.annotation.ExperimentalApi;
import org.density.common.inject.Inject;
import org.density.common.settings.Settings;
import org.density.threadpool.ThreadPool;
import org.density.transport.StreamTransportService;

/**
 *  NodeConnectionsService for StreamTransportService
 */
@ExperimentalApi
public class StreamNodeConnectionsService extends NodeConnectionsService {
    @Inject
    public StreamNodeConnectionsService(Settings settings, ThreadPool threadPool, StreamTransportService streamTransportService) {
        super(settings, threadPool, streamTransportService);
    }
}
