/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.repositories.blobstore;

import org.density.Version;
import org.density.cluster.metadata.IndexMetadata;
import org.density.common.compress.DeflateCompressor;
import org.density.common.settings.Settings;
import org.density.core.common.bytes.BytesReference;
import org.density.core.compress.CompressorRegistry;
import org.density.core.index.Index;
import org.density.test.DensityTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

/**
 * Tests for {@link ChecksumWritableBlobStoreFormat}
 */
public class ChecksumWritableBlobStoreFormatTests extends DensityTestCase {
    private static final String TEST_BLOB_FILE_NAME = "test-blob-name";
    private static final long VERSION = 5L;

    private final ChecksumWritableBlobStoreFormat<IndexMetadata> clusterBlocksFormat = new ChecksumWritableBlobStoreFormat<>(
        "index-metadata",
        IndexMetadata::readFrom
    );

    public void testSerDe() throws IOException {
        IndexMetadata indexMetadata = getIndexMetadata();
        BytesReference bytesReference = clusterBlocksFormat.serialize(
            (out, metadata) -> metadata.writeTo(out),
            indexMetadata,
            TEST_BLOB_FILE_NAME,
            CompressorRegistry.none()
        );
        IndexMetadata readIndexMetadata = clusterBlocksFormat.deserialize(TEST_BLOB_FILE_NAME, bytesReference);
        assertThat(readIndexMetadata, is(indexMetadata));
    }

    public void testSerDeForCompressed() throws IOException {
        IndexMetadata indexMetadata = getIndexMetadata();
        BytesReference bytesReference = clusterBlocksFormat.serialize(
            (out, metadata) -> metadata.writeTo(out),
            indexMetadata,
            TEST_BLOB_FILE_NAME,
            CompressorRegistry.getCompressor(DeflateCompressor.NAME)
        );
        IndexMetadata readIndexMetadata = clusterBlocksFormat.deserialize(TEST_BLOB_FILE_NAME, bytesReference);
        assertThat(readIndexMetadata, is(indexMetadata));
    }

    private IndexMetadata getIndexMetadata() {
        final Index index = new Index("test-index", "index-uuid");
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .build();
        return new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .version(VERSION)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
