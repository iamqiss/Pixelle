/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.remote;

import org.density.core.action.ActionListener;
import org.density.gateway.remote.ClusterMetadataManifest;
import org.density.gateway.remote.model.RemoteReadResult;
import org.density.test.DensityTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AbstractRemoteWritableEntityManagerTests extends DensityTestCase {
    public void testGetStoreWithKnownEntityType() {
        AbstractRemoteWritableEntityManager manager = new ConcreteRemoteWritableEntityManager();
        String knownEntityType = "knownType";
        RemoteWritableEntityStore mockStore = mock(RemoteWritableEntityStore.class);
        manager.remoteWritableEntityStores.put(knownEntityType, mockStore);
        AbstractClusterMetadataWriteableBlobEntity mockEntity = mock(AbstractClusterMetadataWriteableBlobEntity.class);
        when(mockEntity.getType()).thenReturn(knownEntityType);

        RemoteWritableEntityStore store = manager.getStore(mockEntity);
        verify(mockEntity).getType();
        assertEquals(mockStore, store);
    }

    public void testGetStoreWithUnknownEntityType() {
        AbstractRemoteWritableEntityManager manager = new ConcreteRemoteWritableEntityManager();
        String unknownEntityType = "unknownType";
        AbstractClusterMetadataWriteableBlobEntity mockEntity = mock(AbstractClusterMetadataWriteableBlobEntity.class);
        when(mockEntity.getType()).thenReturn(unknownEntityType);

        assertThrows(IllegalArgumentException.class, () -> manager.getStore(mockEntity));
        verify(mockEntity, times(2)).getType();
    }

    private static class ConcreteRemoteWritableEntityManager extends AbstractRemoteWritableEntityManager {
        @Override
        protected ActionListener<Void> getWrappedWriteListener(
            String component,
            AbstractClusterMetadataWriteableBlobEntity remoteEntity,
            ActionListener<ClusterMetadataManifest.UploadedMetadata> listener
        ) {
            return null;
        }

        @Override
        protected ActionListener<Object> getWrappedReadListener(
            String component,
            AbstractClusterMetadataWriteableBlobEntity remoteEntity,
            ActionListener<RemoteReadResult> listener
        ) {
            return null;
        }
    }
}
