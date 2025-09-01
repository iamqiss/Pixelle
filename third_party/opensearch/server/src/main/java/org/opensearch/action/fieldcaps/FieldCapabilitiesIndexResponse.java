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

package org.density.action.fieldcaps;

import org.density.core.action.ActionResponse;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Response for {@link TransportFieldCapabilitiesIndexAction}.
 *
 * @density.internal
 */
public class FieldCapabilitiesIndexResponse extends ActionResponse implements Writeable {
    private final String indexName;
    private final Map<String, IndexFieldCapabilities> responseMap;
    private final boolean canMatch;

    FieldCapabilitiesIndexResponse(String indexName, Map<String, IndexFieldCapabilities> responseMap, boolean canMatch) {
        this.indexName = indexName;
        this.responseMap = responseMap;
        this.canMatch = canMatch;
    }

    FieldCapabilitiesIndexResponse(StreamInput in) throws IOException {
        super(in);
        this.indexName = in.readString();
        this.responseMap = in.readMap(StreamInput::readString, IndexFieldCapabilities::new);
        this.canMatch = in.readBoolean();
    }

    /**
     * Get the index name
     */
    public String getIndexName() {
        return indexName;
    }

    public boolean canMatch() {
        return canMatch;
    }

    /**
     * Get the field capabilities map
     */
    public Map<String, IndexFieldCapabilities> get() {
        return responseMap;
    }

    /**
     *
     * Get the field capabilities for the provided {@code field}
     */
    public IndexFieldCapabilities getField(String field) {
        return responseMap.get(field);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexName);
        out.writeMap(responseMap, StreamOutput::writeString, (valueOut, fc) -> fc.writeTo(valueOut));
        out.writeBoolean(canMatch);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldCapabilitiesIndexResponse that = (FieldCapabilitiesIndexResponse) o;
        return canMatch == that.canMatch && Objects.equals(indexName, that.indexName) && Objects.equals(responseMap, that.responseMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, responseMap, canMatch);
    }
}
