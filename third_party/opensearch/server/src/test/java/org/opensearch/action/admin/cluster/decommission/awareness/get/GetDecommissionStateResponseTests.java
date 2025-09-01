/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.decommission.awareness.get;

import org.density.cluster.decommission.DecommissionStatus;
import org.density.core.xcontent.XContentParser;
import org.density.test.AbstractXContentTestCase;

import java.io.IOException;

public class GetDecommissionStateResponseTests extends AbstractXContentTestCase<GetDecommissionStateResponse> {
    @Override
    protected GetDecommissionStateResponse createTestInstance() {
        DecommissionStatus status = null;
        String attributeValue = null;
        if (randomBoolean()) {
            status = randomFrom(DecommissionStatus.values());
            attributeValue = randomAlphaOfLength(10);
        }
        return new GetDecommissionStateResponse(attributeValue, status);
    }

    @Override
    protected GetDecommissionStateResponse doParseInstance(XContentParser parser) throws IOException {
        return GetDecommissionStateResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
