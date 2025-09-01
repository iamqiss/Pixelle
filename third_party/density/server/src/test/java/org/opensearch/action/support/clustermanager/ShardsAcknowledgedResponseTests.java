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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.density.action.support.clustermanager;

import org.density.Version;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.test.DensityTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.is;

public class ShardsAcknowledgedResponseTests extends DensityTestCase {

    public void testSerialization() throws Exception {
        ShardsAcknowledgedResponse testInstance = new TestImpl(true, true);

        ShardsAcknowledgedResponse result = copyWriteable(
            testInstance,
            new NamedWriteableRegistry(Collections.emptyList()),
            in -> new TestImpl(in, true),
            Version.CURRENT
        );
        assertThat(result.isAcknowledged(), is(true));
        assertThat(result.isShardsAcknowledged(), is(true));
    }

    private static class TestImpl extends ShardsAcknowledgedResponse {

        private TestImpl(StreamInput in, boolean readShardsAcknowledged) throws IOException {
            super(in, readShardsAcknowledged);
        }

        private TestImpl(boolean acknowledged, boolean shardsAcknowledged) {
            super(acknowledged, shardsAcknowledged);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            writeShardsAcknowledged(out);
        }
    }

}
