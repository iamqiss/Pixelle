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

package org.density.index.mapper.size;

import org.apache.lucene.index.IndexableField;
import org.density.common.compress.CompressedXContent;
import org.density.common.settings.Settings;
import org.density.common.xcontent.XContentFactory;
import org.density.core.common.bytes.BytesReference;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.index.IndexService;
import org.density.index.mapper.DocumentMapper;
import org.density.index.mapper.MapperService;
import org.density.index.mapper.ParsedDocument;
import org.density.index.mapper.SourceToParse;
import org.density.plugin.mapper.MapperSizePlugin;
import org.density.plugins.Plugin;
import org.density.test.InternalSettingsPlugin;
import org.density.test.DensitySingleNodeTestCase;

import java.util.Collection;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SizeMappingTests extends DensitySingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(MapperSizePlugin.class, InternalSettingsPlugin.class);
    }

    public void testSizeEnabled() throws Exception {
        IndexService service = createIndexWithSimpleMappings("test", Settings.EMPTY, "_size", "enabled=true");
        DocumentMapper docMapper = service.mapperService().documentMapper();

        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "value").endObject());
        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "1", source, MediaTypeRegistry.JSON));

        boolean stored = false;
        boolean points = false;
        for (IndexableField field : doc.rootDoc().getFields("_size")) {
            stored |= field.fieldType().stored();
            points |= field.fieldType().pointIndexDimensionCount() > 0;
        }
        assertTrue(stored);
        assertTrue(points);
    }

    public void testSizeDisabled() throws Exception {
        IndexService service = createIndexWithSimpleMappings("test", Settings.EMPTY, "_size", "enabled=false");
        DocumentMapper docMapper = service.mapperService().documentMapper();

        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "value").endObject());
        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "1", source, MediaTypeRegistry.JSON));

        assertThat(doc.rootDoc().getField("_size"), nullValue());
    }

    public void testSizeNotSet() throws Exception {
        IndexService service = createIndexWithSimpleMappings("test", Settings.EMPTY);
        DocumentMapper docMapper = service.mapperService().documentMapper();

        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "value").endObject());
        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "1", source, MediaTypeRegistry.JSON));

        assertThat(doc.rootDoc().getField("_size"), nullValue());
    }

    public void testThatDisablingWorksWhenMerging() throws Exception {
        IndexService service = createIndexWithSimpleMappings("test", Settings.EMPTY, "_size", "enabled=true");
        DocumentMapper docMapper = service.mapperService().documentMapper();
        assertThat(docMapper.metadataMapper(SizeFieldMapper.class).enabled(), is(true));

        String disabledMapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("_size")
            .field("enabled", false)
            .endObject()
            .endObject()
            .endObject()
            .toString();
        docMapper = service.mapperService()
            .merge("type", new CompressedXContent(disabledMapping), MapperService.MergeReason.MAPPING_UPDATE);

        assertThat(docMapper.metadataMapper(SizeFieldMapper.class).enabled(), is(false));
    }

}
