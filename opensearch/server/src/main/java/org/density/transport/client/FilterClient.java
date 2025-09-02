/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.transport.client;

import org.density.action.ActionRequest;
import org.density.action.ActionType;
import org.density.common.settings.Settings;
import org.density.core.action.ActionListener;
import org.density.core.action.ActionResponse;
import org.density.threadpool.ThreadPool;
import org.density.transport.client.support.AbstractClient;

/**
 * A {@link Client} that contains another {@link Client} which it
 * uses as its basic source, possibly transforming the requests / responses along the
 * way or providing additional functionality.
 *
 * @density.internal
 */
public abstract class FilterClient extends AbstractClient {

    protected final Client in;

    /**
     * Creates a new FilterClient
     *
     * @param in the client to delegate to
     * @see #in()
     */
    public FilterClient(Client in) {
        this(in.settings(), in.threadPool(), in);
    }

    /**
     * A Constructor that allows to pass settings and threadpool separately. This is useful if the
     * client is a proxy and not yet fully constructed ie. both dependencies are not available yet.
     */
    protected FilterClient(Settings settings, ThreadPool threadPool, Client in) {
        super(settings, threadPool);
        this.in = in;
    }

    @Override
    public void close() {
        in().close();
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        in().execute(action, request, listener);
    }

    /**
     * Returns the delegate {@link Client}
     */
    protected Client in() {
        return in;
    }

    @Override
    public Client getRemoteClusterClient(String clusterAlias) {
        return in.getRemoteClusterClient(clusterAlias);
    }
}
