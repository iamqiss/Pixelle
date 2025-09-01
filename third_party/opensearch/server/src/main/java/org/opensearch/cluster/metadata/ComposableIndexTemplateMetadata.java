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

package org.density.cluster.metadata;

import org.density.Version;
import org.density.cluster.Diff;
import org.density.cluster.DiffableUtils;
import org.density.cluster.NamedDiff;
import org.density.core.ParseField;
import org.density.core.common.Strings;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.xcontent.ConstructingObjectParser;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.core.xcontent.XContentBuilder;
import org.density.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The {@link ComposableIndexTemplateMetadata} class is a custom {@link Metadata.Custom} implementation that
 * stores a map of ids to {@link ComposableIndexTemplate} templates.
 *
 * @density.internal
 */
public class ComposableIndexTemplateMetadata implements Metadata.Custom {
    public static final String TYPE = "index_template";
    private static final ParseField INDEX_TEMPLATE = new ParseField("index_template");
    // minimial supported version when composable templates were introduced
    protected static Version MINIMMAL_SUPPORTED_VERSION = Version.fromId(7070099);
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ComposableIndexTemplateMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        false,
        a -> new ComposableIndexTemplateMetadata((Map<String, ComposableIndexTemplate>) a[0])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, ComposableIndexTemplate> templates = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String name = p.currentName();
                templates.put(name, ComposableIndexTemplate.parse(p));
            }
            return templates;
        }, INDEX_TEMPLATE);
    }

    private final Map<String, ComposableIndexTemplate> indexTemplates;

    public ComposableIndexTemplateMetadata(Map<String, ComposableIndexTemplate> templates) {
        this.indexTemplates = templates;
    }

    public ComposableIndexTemplateMetadata(StreamInput in) throws IOException {
        this.indexTemplates = in.readMap(StreamInput::readString, ComposableIndexTemplate::new);
    }

    public static ComposableIndexTemplateMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public Map<String, ComposableIndexTemplate> indexTemplates() {
        return indexTemplates;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom before) {
        return new ComposableIndexTemplateMetadataDiff((ComposableIndexTemplateMetadata) before, this);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new ComposableIndexTemplateMetadataDiff(in);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return MINIMMAL_SUPPORTED_VERSION;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.indexTemplates, StreamOutput::writeString, (outstream, val) -> val.writeTo(outstream));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(INDEX_TEMPLATE.getPreferredName());
        for (Map.Entry<String, ComposableIndexTemplate> template : indexTemplates.entrySet()) {
            builder.field(template.getKey(), template.getValue());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.indexTemplates);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ComposableIndexTemplateMetadata other = (ComposableIndexTemplateMetadata) obj;
        return Objects.equals(this.indexTemplates, other.indexTemplates);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    /**
     * A diff between composable metadata templates.
     *
     * @density.internal
     */
    static class ComposableIndexTemplateMetadataDiff implements NamedDiff<Metadata.Custom> {

        final Diff<Map<String, ComposableIndexTemplate>> indexTemplateDiff;

        ComposableIndexTemplateMetadataDiff(ComposableIndexTemplateMetadata before, ComposableIndexTemplateMetadata after) {
            this.indexTemplateDiff = DiffableUtils.diff(
                before.indexTemplates,
                after.indexTemplates,
                DiffableUtils.getStringKeySerializer()
            );
        }

        ComposableIndexTemplateMetadataDiff(StreamInput in) throws IOException {
            this.indexTemplateDiff = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                ComposableIndexTemplate::new,
                ComposableIndexTemplate::readITV2DiffFrom
            );
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new ComposableIndexTemplateMetadata(indexTemplateDiff.apply(((ComposableIndexTemplateMetadata) part).indexTemplates));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            indexTemplateDiff.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }
    }
}
