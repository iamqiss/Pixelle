/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.density.common.compress.CompressedXContent;
import org.density.common.settings.Settings;
import org.density.core.common.bytes.BytesArray;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.test.DensitySingleNodeTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/** tests for {@link org.density.index.mapper.NestedPathFieldMapper} */
public class NestedPathFieldMapperTests extends DensitySingleNodeTestCase {

    public void testDefaultConfig() throws IOException {
        Settings indexSettings = Settings.EMPTY;
        MapperService mapperService = createIndex("test", indexSettings).mapperService();
        DocumentMapper mapper = mapperService.merge(
            MapperService.SINGLE_MAPPING_NAME,
            new CompressedXContent("{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{}}"),
            MapperService.MergeReason.MAPPING_UPDATE
        );
        ParsedDocument document = mapper.parse(new SourceToParse("index", "id", new BytesArray("{}"), MediaTypeRegistry.JSON));
        assertEquals(Collections.<IndexableField>emptyList(), Arrays.asList(document.rootDoc().getFields(NestedPathFieldMapper.NAME)));
    }

    public void testUpdatesWithSameMappings() throws IOException {
        Settings indexSettings = Settings.EMPTY;
        MapperService mapperService = createIndex("test", indexSettings).mapperService();
        DocumentMapper mapper = mapperService.merge(
            MapperService.SINGLE_MAPPING_NAME,
            new CompressedXContent("{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{}}"),
            MapperService.MergeReason.MAPPING_UPDATE
        );
        mapper.merge(mapper.mapping(), MapperService.MergeReason.MAPPING_UPDATE);
    }
}
