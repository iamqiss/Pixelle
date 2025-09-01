/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.translog.transfer;

import org.density.common.io.IndexIOStreamHandler;
import org.density.test.DensityTestCase;
import org.junit.Before;

/**
 * Unit tests for {@link org.density.index.translog.transfer.TranslogTransferMetadataHandlerFactoryTests}.
 */
public class TranslogTransferMetadataHandlerFactoryTests extends DensityTestCase {

    private TranslogTransferMetadataHandlerFactory translogTransferMetadataHandlerFactory;

    @Before
    public void setup() {
        translogTransferMetadataHandlerFactory = new TranslogTransferMetadataHandlerFactory();
    }

    public void testGetHandlerReturnsBasedOnVersion() {
        IndexIOStreamHandler<TranslogTransferMetadata> versionOneHandler = translogTransferMetadataHandlerFactory.getHandler(1);
        assertTrue(versionOneHandler instanceof TranslogTransferMetadataHandler);
    }

    public void testGetHandlerWhenCalledMultipleTimesReturnsCachedHandler() {
        IndexIOStreamHandler<TranslogTransferMetadata> versionTwoHandlerOne = translogTransferMetadataHandlerFactory.getHandler(1);
        IndexIOStreamHandler<TranslogTransferMetadata> versionTwoHandlerTwo = translogTransferMetadataHandlerFactory.getHandler(1);
        assertEquals(versionTwoHandlerOne, versionTwoHandlerTwo);
    }

    public void testGetHandlerWhenHandlerNotProvidedThrowsException() {
        Throwable throwable = assertThrows(IllegalArgumentException.class, () -> { translogTransferMetadataHandlerFactory.getHandler(2); });
        assertEquals("Unsupported TranslogTransferMetadata version: 2", throwable.getMessage());
    }
}
