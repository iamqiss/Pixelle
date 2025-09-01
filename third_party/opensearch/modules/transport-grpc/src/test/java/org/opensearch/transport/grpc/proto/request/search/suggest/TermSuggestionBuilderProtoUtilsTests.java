/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.grpc.proto.request.search.suggest;

import org.density.protobufs.SearchRequest;
import org.density.search.suggest.term.TermSuggestionBuilder;
import org.density.test.DensityTestCase;

public class TermSuggestionBuilderProtoUtilsTests extends DensityTestCase {

    public void testResolveWithAlwaysMode() {
        // Call the method under test with ALWAYS mode
        TermSuggestionBuilder.SuggestMode result = TermSuggestionBuilderProtoUtils.resolve(SearchRequest.SuggestMode.SUGGEST_MODE_ALWAYS);

        // Verify the result
        assertEquals("SuggestMode should be ALWAYS", TermSuggestionBuilder.SuggestMode.ALWAYS, result);
    }

    public void testResolveWithMissingMode() {
        // Call the method under test with MISSING mode
        TermSuggestionBuilder.SuggestMode result = TermSuggestionBuilderProtoUtils.resolve(SearchRequest.SuggestMode.SUGGEST_MODE_MISSING);

        // Verify the result
        assertEquals("SuggestMode should be MISSING", TermSuggestionBuilder.SuggestMode.MISSING, result);
    }

    public void testResolveWithPopularMode() {
        // Call the method under test with POPULAR mode
        TermSuggestionBuilder.SuggestMode result = TermSuggestionBuilderProtoUtils.resolve(SearchRequest.SuggestMode.SUGGEST_MODE_POPULAR);

        // Verify the result
        assertEquals("SuggestMode should be POPULAR", TermSuggestionBuilder.SuggestMode.POPULAR, result);
    }

    public void testResolveWithInvalidMode() {
        // Call the method under test with UNRECOGNIZED mode, should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> TermSuggestionBuilderProtoUtils.resolve(SearchRequest.SuggestMode.UNRECOGNIZED)
        );

        // Verify the exception message
        assertTrue("Exception message should mention invalid suggest_mode", exception.getMessage().contains("Invalid suggest_mode"));
    }
}
