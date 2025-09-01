/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.grpc.proto.response.exceptions;

import org.density.action.search.SearchPhaseExecutionException;
import org.density.action.search.ShardSearchFailure;
import org.density.protobufs.ObjectMap;
import org.density.test.DensityTestCase;

import java.util.Map;

public class SearchPhaseExecutionExceptionProtoUtilsTests extends DensityTestCase {

    public void testMetadataToProto() {
        // Create a SearchPhaseExecutionException with specific values
        String phaseName = "query";

        // Create a ShardSearchFailure
        RuntimeException cause = new RuntimeException("Test cause");
        ShardSearchFailure[] shardFailures = new ShardSearchFailure[] { new ShardSearchFailure(cause) };

        SearchPhaseExecutionException exception = new SearchPhaseExecutionException(
            phaseName,
            "Test search phase execution error",
            shardFailures
        );

        // Convert to Protocol Buffer
        Map<String, ObjectMap.Value> metadata = SearchPhaseExecutionExceptionProtoUtils.metadataToProto(exception);

        // Verify the conversion
        assertTrue("Should have phase field", metadata.containsKey("phase"));
        assertTrue("Should have grouped field", metadata.containsKey("grouped"));
        assertTrue("Should have failed_shards field", metadata.containsKey("failed_shards"));

        // Verify field values
        ObjectMap.Value phaseValue = metadata.get("phase");
        ObjectMap.Value groupedValue = metadata.get("grouped");
        ObjectMap.Value failedShardsValue = metadata.get("failed_shards");

        assertEquals("phase should match", phaseName, phaseValue.getString());
        assertTrue("grouped should be true", groupedValue.getBool());

        // Verify failed_shards list
        ObjectMap.ListValue failedShardsList = failedShardsValue.getListValue();
        assertEquals("failed_shards should have 1 item", 1, failedShardsList.getValueCount());

        // Note: Since ShardOperationFailedExceptionProtoUtils.toProto() returns an empty Value,
        // we can't verify the content of the failed shard item beyond its existence
    }
}
