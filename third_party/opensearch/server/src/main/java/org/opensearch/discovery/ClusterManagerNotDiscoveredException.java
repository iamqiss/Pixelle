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

package org.density.discovery;

import org.density.DensityException;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.rest.RestStatus;

import java.io.IOException;

/**
 * Exception when the cluster-manager is not discovered
 *
 * @density.internal
 */
public class ClusterManagerNotDiscoveredException extends DensityException {

    public ClusterManagerNotDiscoveredException() {
        super("");
    }

    public ClusterManagerNotDiscoveredException(Throwable cause) {
        super(cause);
    }

    public ClusterManagerNotDiscoveredException(String message) {
        super(message);
    }

    @Override
    public RestStatus status() {
        return RestStatus.SERVICE_UNAVAILABLE;
    }

    public ClusterManagerNotDiscoveredException(StreamInput in) throws IOException {
        super(in);
    }
}
