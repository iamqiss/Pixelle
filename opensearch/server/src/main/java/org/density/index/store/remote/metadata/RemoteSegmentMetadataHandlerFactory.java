/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.store.remote.metadata;

import org.density.common.io.IndexIOStreamHandler;
import org.density.common.io.IndexIOStreamHandlerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link RemoteSegmentMetadataHandlerFactory} is a factory class to create {@link RemoteSegmentMetadataHandler}
 * instances based on the {@link RemoteSegmentMetadata} version
 *
 * @density.internal
 */
public class RemoteSegmentMetadataHandlerFactory implements IndexIOStreamHandlerFactory<RemoteSegmentMetadata> {
    private final AtomicReference<IndexIOStreamHandler<RemoteSegmentMetadata>> handlerRef = new AtomicReference<>();

    @Override
    public IndexIOStreamHandler<RemoteSegmentMetadata> getHandler(int version) {
        IndexIOStreamHandler<RemoteSegmentMetadata> current = handlerRef.get();
        if (current != null) {
            return current;
        }

        IndexIOStreamHandler<RemoteSegmentMetadata> newHandler = createHandler(version);
        handlerRef.compareAndSet(null, newHandler);
        return handlerRef.get();
    }

    private IndexIOStreamHandler<RemoteSegmentMetadata> createHandler(int version) {
        return switch (version) {
            case RemoteSegmentMetadata.VERSION_ONE -> new RemoteSegmentMetadataHandler(RemoteSegmentMetadata.VERSION_ONE);
            case RemoteSegmentMetadata.VERSION_TWO -> new RemoteSegmentMetadataHandler(RemoteSegmentMetadata.VERSION_TWO);
            default -> throw new IllegalArgumentException("Unsupported RemoteSegmentMetadata version: " + version);
        };
    }
}
