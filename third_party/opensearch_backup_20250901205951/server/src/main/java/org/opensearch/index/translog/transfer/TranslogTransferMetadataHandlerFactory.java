/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.translog.transfer;

import org.density.common.io.IndexIOStreamHandler;
import org.density.common.io.IndexIOStreamHandlerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link TranslogTransferMetadataHandlerFactory} is a factory class to create {@link TranslogTransferMetadataHandler}
 * instances based on the {@link TranslogTransferMetadata} version
 *
 * @density.internal
 */
public class TranslogTransferMetadataHandlerFactory implements IndexIOStreamHandlerFactory<TranslogTransferMetadata> {

    private final ConcurrentHashMap<Integer, IndexIOStreamHandler<TranslogTransferMetadata>> handlers = new ConcurrentHashMap<>();

    @Override
    public IndexIOStreamHandler<TranslogTransferMetadata> getHandler(int version) {
        return handlers.computeIfAbsent(version, this::createHandler);
    }

    private IndexIOStreamHandler<TranslogTransferMetadata> createHandler(int version) {
        return switch (version) {
            case TranslogTransferMetadata.CURRENT_VERSION -> new TranslogTransferMetadataHandler();
            default -> throw new IllegalArgumentException("Unsupported TranslogTransferMetadata version: " + version);
        };
    }
}
