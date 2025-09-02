/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.search.profile.fetch;

import org.density.common.xcontent.XContentType;
import org.density.core.common.bytes.BytesReference;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentParser;
import org.density.core.xcontent.XContentParserUtils;
import org.density.search.profile.ProfileResult;
import org.density.search.profile.ProfileResultTests;
import org.density.test.DensityTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.density.core.xcontent.XContentHelper.toXContent;
import static org.density.test.hamcrest.DensityAssertions.assertToXContentEquivalent;

public class FetchProfileShardResultTests extends DensityTestCase {

    public static FetchProfileShardResult createTestItem() {
        int size = randomIntBetween(0, 5);
        List<ProfileResult> results = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            results.add(ProfileResultTests.createTestItem(1, false));
        }
        return new FetchProfileShardResult(results);
    }

    public void testFromXContent() throws IOException {
        FetchProfileShardResult profileResult = createTestItem();
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(profileResult, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        FetchProfileShardResult parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            XContentParserUtils.ensureFieldName(parser, parser.nextToken(), FetchProfileShardResult.FETCH);
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser);
            parsed = FetchProfileShardResult.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);
    }

    public void testGetFetchProfileResults() {
        List<ProfileResult> expected = new ArrayList<>();
        expected.add(new ProfileResult("type1", "desc1", Map.of("key1", 1L), Map.of(), 10L, List.of()));
        expected.add(new ProfileResult("type2", "desc2", Map.of("key2", 2L), Map.of(), 20L, List.of()));

        FetchProfileShardResult shardResult = new FetchProfileShardResult(expected);

        assertEquals(expected, shardResult.getFetchProfileResults());
    }
}
