/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.remotestore.stats;

import org.density.common.io.stream.BytesStreamOutput;
import org.density.core.common.io.stream.StreamInput;
import org.density.test.DensityTestCase;

import static org.hamcrest.Matchers.equalTo;

public class RemoteStoreStatsRequestTests extends DensityTestCase {

    public void testAddIndexName() throws Exception {
        RemoteStoreStatsRequest request = new RemoteStoreStatsRequest();
        request.indices("random-index-name");
        RemoteStoreStatsRequest deserializedRequest = roundTripRequest(request);
        assertRequestsEqual(request, deserializedRequest);
    }

    public void testAddShardId() throws Exception {
        RemoteStoreStatsRequest request = new RemoteStoreStatsRequest();
        request.indices("random-index-name");
        request.shards("0");
        RemoteStoreStatsRequest deserializedRequest = roundTripRequest(request);
        assertRequestsEqual(request, deserializedRequest);
    }

    public void testAddLocalShardsOnly() throws Exception {
        RemoteStoreStatsRequest request = new RemoteStoreStatsRequest();
        request.indices("random-index-name");
        request.local(true);
        RemoteStoreStatsRequest deserializedRequest = roundTripRequest(request);
        assertRequestsEqual(request, deserializedRequest);
    }

    public void testAddShardIdAndLocalShardsOnly() throws Exception {
        RemoteStoreStatsRequest request = new RemoteStoreStatsRequest();
        request.indices("random-index-name");
        request.shards("0");
        request.local(true);
        RemoteStoreStatsRequest deserializedRequest = roundTripRequest(request);
        assertRequestsEqual(request, deserializedRequest);
    }

    /**
     * Serialize and deserialize a request.
     * @param request A request to serialize.
     * @return The deserialized, "round-tripped" request.
     */
    private static RemoteStoreStatsRequest roundTripRequest(RemoteStoreStatsRequest request) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new RemoteStoreStatsRequest(in);
            }
        }
    }

    private static void assertRequestsEqual(RemoteStoreStatsRequest request1, RemoteStoreStatsRequest request2) {
        assertThat(request1.indices(), equalTo(request2.indices()));
        assertThat(request1.shards(), equalTo(request2.shards()));
    }
}
