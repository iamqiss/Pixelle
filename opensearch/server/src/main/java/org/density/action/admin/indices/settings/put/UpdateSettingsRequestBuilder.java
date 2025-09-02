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

package org.density.action.admin.indices.settings.put;

import org.density.action.support.IndicesOptions;
import org.density.action.support.clustermanager.AcknowledgedRequestBuilder;
import org.density.action.support.clustermanager.AcknowledgedResponse;
import org.density.common.annotation.PublicApi;
import org.density.common.settings.Settings;
import org.density.common.xcontent.XContentType;
import org.density.transport.client.DensityClient;

import java.util.Map;

/**
 * Builder for an update index settings request
 *
 * @density.api
 */
@PublicApi(since = "1.0.0")
public class UpdateSettingsRequestBuilder extends AcknowledgedRequestBuilder<
    UpdateSettingsRequest,
    AcknowledgedResponse,
    UpdateSettingsRequestBuilder> {

    public UpdateSettingsRequestBuilder(DensityClient client, UpdateSettingsAction action, String... indices) {
        super(client, action, new UpdateSettingsRequest(indices));
    }

    /**
     * Sets the indices the update settings will execute on
     */
    public UpdateSettingsRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     * <p>
     * For example indices that don't exist.
     */
    public UpdateSettingsRequestBuilder setIndicesOptions(IndicesOptions options) {
        request.indicesOptions(options);
        return this;
    }

    /**
     * Sets the settings to be updated
     */
    public UpdateSettingsRequestBuilder setSettings(Settings settings) {
        request.settings(settings);
        return this;
    }

    /**
     * Sets the settings to be updated
     */
    public UpdateSettingsRequestBuilder setSettings(Settings.Builder settings) {
        request.settings(settings);
        return this;
    }

    /**
     * Sets the settings to be updated (either json or yaml format)
     */
    public UpdateSettingsRequestBuilder setSettings(String source, XContentType xContentType) {
        request.settings(source, xContentType);
        return this;
    }

    /**
     * Sets the settings to be updated
     */
    public UpdateSettingsRequestBuilder setSettings(Map<String, Object> source) {
        request.settings(source);
        return this;
    }

    public UpdateSettingsRequestBuilder setPreserveExisting(boolean preserveExisting) {
        request.setPreserveExisting(preserveExisting);
        return this;
    }
}
