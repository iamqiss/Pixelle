/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.indices.recovery;

import org.density.Version;
import org.density.cluster.node.DiscoveryNode;
import org.density.common.UUIDs;
import org.density.core.common.io.stream.InputStreamStreamInput;
import org.density.core.common.io.stream.OutputStreamStreamOutput;
import org.density.core.index.shard.ShardId;
import org.density.index.engine.Engine;
import org.density.index.seqno.SequenceNumbers;
import org.density.index.store.Store;
import org.density.test.DensityTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collections;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.density.test.VersionUtils.randomVersion;
import static org.hamcrest.Matchers.equalTo;

public class StartRecoveryRequestTests extends DensityTestCase {

    public void testSerialization() throws Exception {
        final Version targetNodeVersion = randomVersion(random());
        Store.MetadataSnapshot metadataSnapshot = randomBoolean()
            ? Store.MetadataSnapshot.EMPTY
            : new Store.MetadataSnapshot(
                Collections.emptyMap(),
                Collections.singletonMap(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID()),
                randomIntBetween(0, 100)
            );
        final StartRecoveryRequest outRequest = new StartRecoveryRequest(
            new ShardId("test", "_na_", 0),
            UUIDs.randomBase64UUID(),
            new DiscoveryNode("a", buildNewFakeTransportAddress(), emptyMap(), emptySet(), targetNodeVersion),
            new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), targetNodeVersion),
            metadataSnapshot,
            randomBoolean(),
            randomNonNegativeLong(),
            randomBoolean() || metadataSnapshot.getHistoryUUID() == null ? SequenceNumbers.UNASSIGNED_SEQ_NO : randomNonNegativeLong()
        );

        final ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        final OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        out.setVersion(targetNodeVersion);
        outRequest.writeTo(out);

        final ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
        in.setVersion(targetNodeVersion);
        final StartRecoveryRequest inRequest = new StartRecoveryRequest(in);

        assertThat(outRequest.shardId(), equalTo(inRequest.shardId()));
        assertThat(outRequest.targetAllocationId(), equalTo(inRequest.targetAllocationId()));
        assertThat(outRequest.sourceNode(), equalTo(inRequest.sourceNode()));
        assertThat(outRequest.targetNode(), equalTo(inRequest.targetNode()));
        assertThat(outRequest.metadataSnapshot().asMap(), equalTo(inRequest.metadataSnapshot().asMap()));
        assertThat(outRequest.isPrimaryRelocation(), equalTo(inRequest.isPrimaryRelocation()));
        assertThat(outRequest.recoveryId(), equalTo(inRequest.recoveryId()));
        assertThat(outRequest.startingSeqNo(), equalTo(inRequest.startingSeqNo()));
    }

}
