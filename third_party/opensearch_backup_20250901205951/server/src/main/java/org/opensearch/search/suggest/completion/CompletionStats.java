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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.density.search.suggest.completion;

import org.density.common.FieldMemoryStats;
import org.density.common.Nullable;
import org.density.common.annotation.PublicApi;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.io.stream.Writeable;
import org.density.core.common.unit.ByteSizeValue;
import org.density.core.xcontent.ToXContentFragment;
import org.density.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Stats for completion suggester
 *
 * @density.api
 */
@PublicApi(since = "1.0.0")
public class CompletionStats implements Writeable, ToXContentFragment {

    private static final String COMPLETION = "completion";
    private static final String SIZE_IN_BYTES = "size_in_bytes";
    private static final String SIZE = "size";
    private static final String FIELDS = "fields";

    private long sizeInBytes;
    @Nullable
    private FieldMemoryStats fields;

    public CompletionStats() {}

    public CompletionStats(StreamInput in) throws IOException {
        sizeInBytes = in.readVLong();
        fields = in.readOptionalWriteable(FieldMemoryStats::new);
    }

    public CompletionStats(long size, @Nullable FieldMemoryStats fields) {
        this.sizeInBytes = size;
        this.fields = fields;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public ByteSizeValue getSize() {
        return new ByteSizeValue(sizeInBytes);
    }

    public FieldMemoryStats getFields() {
        return fields;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(sizeInBytes);
        out.writeOptionalWriteable(fields);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(COMPLETION);
        builder.humanReadableField(SIZE_IN_BYTES, SIZE, getSize());
        if (fields != null) {
            fields.toXContent(builder, FIELDS, SIZE_IN_BYTES, SIZE);
        }
        builder.endObject();
        return builder;
    }

    public void add(CompletionStats completion) {
        if (completion == null) {
            return;
        }
        sizeInBytes += completion.getSizeInBytes();
        if (completion.fields != null) {
            if (fields == null) {
                fields = completion.fields.copy();
            } else {
                fields.add(completion.fields);
            }
        }
    }
}
