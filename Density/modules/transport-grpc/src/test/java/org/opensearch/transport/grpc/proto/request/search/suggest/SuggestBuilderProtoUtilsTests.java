/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.grpc.proto.request.search.suggest;

import org.density.protobufs.Suggester;
import org.density.search.suggest.SuggestBuilder;
import org.density.test.DensityTestCase;

public class SuggestBuilderProtoUtilsTests extends DensityTestCase {

    public void testFromProtoWithEmptySuggester() {
        // Create an empty Suggester proto
        Suggester suggesterProto = Suggester.newBuilder().build();

        // Call the method under test
        SuggestBuilder suggestBuilder = SuggestBuilderProtoUtils.fromProto(suggesterProto);

        // Verify the result
        assertNotNull("SuggestBuilder should not be null", suggestBuilder);
    }
}
