/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.pipeline;

import org.density.Version;
import org.density.common.io.stream.BytesStreamOutput;
import org.density.core.common.io.stream.StreamInput;
import org.density.test.DensityTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SearchPipelineInfoTests extends DensityTestCase {
    public void testSerializationRoundtrip() throws IOException {
        SearchPipelineInfo searchPipelineInfo = new SearchPipelineInfo(
            Map.of(
                "a",
                List.of(new ProcessorInfo("a1"), new ProcessorInfo("a2"), new ProcessorInfo("a3")),
                "b",
                List.of(new ProcessorInfo("b1"), new ProcessorInfo("b2")),
                "c",
                List.of(new ProcessorInfo("c1"))
            )
        );
        SearchPipelineInfo deserialized;
        try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()) {
            searchPipelineInfo.writeTo(bytesStreamOutput);
            try (StreamInput bytesStreamInput = bytesStreamOutput.bytes().streamInput()) {
                deserialized = new SearchPipelineInfo(bytesStreamInput);
            }
        }
        assertTrue(deserialized.containsProcessor("a", "a1"));
        assertTrue(deserialized.containsProcessor("a", "a2"));
        assertTrue(deserialized.containsProcessor("a", "a3"));
        assertTrue(deserialized.containsProcessor("b", "b1"));
        assertTrue(deserialized.containsProcessor("b", "b2"));
        assertTrue(deserialized.containsProcessor("c", "c1"));
    }

    /**
     * When serializing / deserializing to / from old versions, processor type info is lost.
     * <p>
     * Also, we only supported request/response processors.
     */
    public void testSerializationRoundtripBackcompat() throws IOException {
        SearchPipelineInfo searchPipelineInfo = new SearchPipelineInfo(
            Map.of(
                Pipeline.REQUEST_PROCESSORS_KEY,
                List.of(new ProcessorInfo("a1"), new ProcessorInfo("a2"), new ProcessorInfo("a3")),
                Pipeline.RESPONSE_PROCESSORS_KEY,
                List.of(new ProcessorInfo("b1"), new ProcessorInfo("b2"))
            )
        );
        SearchPipelineInfo deserialized;
        try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()) {
            bytesStreamOutput.setVersion(Version.V_2_7_0);
            searchPipelineInfo.writeTo(bytesStreamOutput);
            try (StreamInput bytesStreamInput = bytesStreamOutput.bytes().streamInput()) {
                bytesStreamInput.setVersion(Version.V_2_7_0);
                deserialized = new SearchPipelineInfo(bytesStreamInput);
            }
        }
        for (String proc : List.of("a1", "a2", "a3", "b1", "b2")) {
            assertTrue(deserialized.containsProcessor(Pipeline.REQUEST_PROCESSORS_KEY, proc));
            assertTrue(deserialized.containsProcessor(Pipeline.RESPONSE_PROCESSORS_KEY, proc));
        }
    }
}
