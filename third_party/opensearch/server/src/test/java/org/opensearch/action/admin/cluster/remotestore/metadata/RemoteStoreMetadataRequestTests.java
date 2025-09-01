/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.remotestore.metadata;

import org.density.common.io.stream.BytesStreamOutput;
import org.density.core.common.io.stream.StreamInput;
import org.density.test.DensityTestCase;
import org.hamcrest.MatcherAssert;

import static org.hamcrest.Matchers.equalTo;

public class RemoteStoreMetadataRequestTests extends DensityTestCase {

    public void testAddIndexName() throws Exception {
        RemoteStoreMetadataRequest request = new RemoteStoreMetadataRequest();
        request.indices("test-index");
        RemoteStoreMetadataRequest deserializedRequest = roundTripRequest(request);
        assertRequestsEqual(request, deserializedRequest);
    }

    public void testAddShardId() throws Exception {
        RemoteStoreMetadataRequest request = new RemoteStoreMetadataRequest();
        request.indices("test-index");
        request.shards("0");
        RemoteStoreMetadataRequest deserializedRequest = roundTripRequest(request);
        assertRequestsEqual(request, deserializedRequest);
    }

    public void testAddMultipleShards() throws Exception {
        RemoteStoreMetadataRequest request = new RemoteStoreMetadataRequest();
        request.indices("test-index");
        request.shards("0", "1", "2");
        RemoteStoreMetadataRequest deserializedRequest = roundTripRequest(request);
        assertRequestsEqual(request, deserializedRequest);
    }

    private static RemoteStoreMetadataRequest roundTripRequest(RemoteStoreMetadataRequest request) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new RemoteStoreMetadataRequest(in);
            }
        }
    }

    private static void assertRequestsEqual(RemoteStoreMetadataRequest request1, RemoteStoreMetadataRequest request2) {
        MatcherAssert.assertThat(request1.indices(), equalTo(request2.indices()));
        MatcherAssert.assertThat(request1.shards(), equalTo(request2.shards()));
    }
}
