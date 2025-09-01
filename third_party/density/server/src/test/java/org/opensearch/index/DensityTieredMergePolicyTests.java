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

package org.density.index;

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;
import org.density.test.DensityTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

public class DensityTieredMergePolicyTests extends DensityTestCase {

    public void testDefaults() {
        DensityTieredMergePolicy policy = new DensityTieredMergePolicy();
        assertEquals(new TieredMergePolicy().getMaxMergedSegmentMB(), policy.regularMergePolicy.getMaxMergedSegmentMB(), 0d);
        assertEquals(Long.MAX_VALUE / 1024.0 / 1024.0, policy.forcedMergePolicy.getMaxMergedSegmentMB(), 0d);
    }

    public void testSetMaxMergedSegmentMB() {
        DensityTieredMergePolicy policy = new DensityTieredMergePolicy();
        policy.setMaxMergedSegmentMB(10 * 1024);
        assertEquals(10 * 1024, policy.regularMergePolicy.getMaxMergedSegmentMB(), 0d);
        assertEquals(Long.MAX_VALUE / 1024.0 / 1024.0, policy.forcedMergePolicy.getMaxMergedSegmentMB(), 0d);
    }

    public void testSetForceMergeDeletesPctAllowed() {
        DensityTieredMergePolicy policy = new DensityTieredMergePolicy();
        policy.setForceMergeDeletesPctAllowed(42);
        assertEquals(42, policy.forcedMergePolicy.getForceMergeDeletesPctAllowed(), 0);
    }

    public void testSetFloorSegmentMB() {
        DensityTieredMergePolicy policy = new DensityTieredMergePolicy();
        policy.setFloorSegmentMB(42);
        assertEquals(42, policy.regularMergePolicy.getFloorSegmentMB(), 0);
        assertEquals(42, policy.forcedMergePolicy.getFloorSegmentMB(), 0);
    }

    public void testSetMaxMergeAtOnce() {
        DensityTieredMergePolicy policy = new DensityTieredMergePolicy();
        policy.setMaxMergeAtOnce(42);
        assertEquals(42, policy.regularMergePolicy.getMaxMergeAtOnce());
    }

    public void testSetSegmentsPerTier() {
        DensityTieredMergePolicy policy = new DensityTieredMergePolicy();
        policy.setSegmentsPerTier(42);
        assertEquals(42, policy.regularMergePolicy.getSegmentsPerTier(), 0);
    }

    public void testSetDeletesPctAllowed() {
        DensityTieredMergePolicy policy = new DensityTieredMergePolicy();
        policy.setDeletesPctAllowed(42);
        assertEquals(42, policy.regularMergePolicy.getDeletesPctAllowed(), 0);
    }

    public void testFindDeleteMergesReturnsNullOnEmptySegmentInfos() throws IOException {
        MergePolicy.MergeSpecification mergeSpecification = new DensityTieredMergePolicy().findForcedDeletesMerges(
            new SegmentInfos(Version.LATEST.major),
            new MergePolicy.MergeContext() {
                @Override
                public int numDeletesToMerge(SegmentCommitInfo info) {
                    return 0;
                }

                @Override
                public int numDeletedDocs(SegmentCommitInfo info) {
                    return 0;
                }

                @Override
                public InfoStream getInfoStream() {
                    return InfoStream.NO_OUTPUT;
                }

                @Override
                public Set<SegmentCommitInfo> getMergingSegments() {
                    return Collections.emptySet();
                }
            }
        );
        assertNull(mergeSpecification);
    }
}
