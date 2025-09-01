/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.remotestore.metadata;

import org.density.common.unit.TimeValue;
import org.density.test.DensityTestCase;
import org.density.transport.client.DensityClient;

import static org.mockito.Mockito.mock;

public class RemoteStoreMetadataRequestBuilderTests extends DensityTestCase {

    public void testSetTimeout() {
        DensityClient mockClient = mock(DensityClient.class);
        RemoteStoreMetadataRequestBuilder builder = new RemoteStoreMetadataRequestBuilder(mockClient, RemoteStoreMetadataAction.INSTANCE);

        TimeValue timeout = TimeValue.timeValueSeconds(15);
        builder.setTimeout(timeout);

        assertEquals(timeout, builder.request().timeout());
    }

    public void testSetShards() {
        DensityClient mockClient = mock(DensityClient.class);
        RemoteStoreMetadataRequestBuilder builder = new RemoteStoreMetadataRequestBuilder(mockClient, RemoteStoreMetadataAction.INSTANCE);

        String[] shards = new String[] { "0", "1" };
        builder.setShards(shards);

        assertArrayEquals(shards, builder.request().shards());
    }

    public void testChaining() {
        DensityClient mockClient = mock(DensityClient.class);
        RemoteStoreMetadataRequestBuilder builder = new RemoteStoreMetadataRequestBuilder(mockClient, RemoteStoreMetadataAction.INSTANCE);

        TimeValue timeout = TimeValue.timeValueSeconds(10);
        String[] shards = new String[] { "0", "2" };

        RemoteStoreMetadataRequestBuilder returned = builder.setTimeout(timeout).setShards(shards);

        assertSame(builder, returned);
        assertEquals(timeout, returned.request().timeout());
        assertArrayEquals(shards, returned.request().shards());
    }
}
