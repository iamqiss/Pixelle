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

package org.density.client.tasks;

import org.density.client.AbstractResponseTestCase;
import org.density.common.xcontent.XContentType;
import org.density.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;

public class DensityExceptionTests extends AbstractResponseTestCase<
    org.density.DensityException,
    org.density.client.tasks.DensityException> {

    @Override
    protected org.density.DensityException createServerTestInstance(XContentType xContentType) {
        IllegalStateException ies = new IllegalStateException("illegal_state");
        IllegalArgumentException iae = new IllegalArgumentException("argument", ies);
        org.density.DensityException exception = new org.density.DensityException("elastic_exception", iae);
        exception.addHeader("key", "value");
        exception.addMetadata("density.meta", "data");
        exception.addSuppressed(new NumberFormatException("3/0"));
        return exception;
    }

    @Override
    protected DensityException doParseToClientInstance(XContentParser parser) throws IOException {
        parser.nextToken();
        return DensityException.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.density.DensityException serverTestInstance, DensityException clientInstance) {

        IllegalArgumentException sCauseLevel1 = (IllegalArgumentException) serverTestInstance.getCause();
        DensityException cCauseLevel1 = clientInstance.getCause();

        assertTrue(sCauseLevel1 != null);
        assertTrue(cCauseLevel1 != null);

        IllegalStateException causeLevel2 = (IllegalStateException) serverTestInstance.getCause().getCause();
        DensityException cCauseLevel2 = clientInstance.getCause().getCause();
        assertTrue(causeLevel2 != null);
        assertTrue(cCauseLevel2 != null);

        DensityException cause = new DensityException("Density exception [type=illegal_state_exception, reason=illegal_state]");
        DensityException caused1 = new DensityException(
            "Density exception [type=illegal_argument_exception, reason=argument]",
            cause
        );
        DensityException caused2 = new DensityException("Density exception [type=exception, reason=elastic_exception]", caused1);

        caused2.addHeader("key", Collections.singletonList("value"));
        DensityException supp = new DensityException("Density exception [type=number_format_exception, reason=3/0]");
        caused2.addSuppressed(Collections.singletonList(supp));

        assertEquals(caused2, clientInstance);

    }

}
