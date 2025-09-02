/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.example.systemingestprocessor;

import org.density.ingest.IngestDocument;
import org.density.ingest.IngestDocumentWrapper;
import org.density.test.DensityTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;

import static org.density.example.systemingestprocessor.ExampleSystemIngestProcessor.AUTO_ADDED_FIELD_NAME;
import static org.density.example.systemingestprocessor.ExampleSystemIngestProcessor.AUTO_ADDED_FIELD_VALUE;

public class ExampleSystemIngestProcessorTests extends DensityTestCase {
    private final ExampleSystemIngestProcessor exampleSystemIngestProcessor = new ExampleSystemIngestProcessor("tag", "description", 10);

    public void testExecute() {
        final IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());

        exampleSystemIngestProcessor.execute(ingestDocument);

        assertEquals(AUTO_ADDED_FIELD_VALUE, ingestDocument.getSourceAndMetadata().get(AUTO_ADDED_FIELD_NAME));
    }

    public void testSubBatchExecute() {
        final IngestDocumentWrapper ingestDocumentWrapper1 = createIngestDocumentWrapper(1);
        final IngestDocumentWrapper ingestDocumentWrapper2 = createIngestDocumentWrapper(2);
        final List<IngestDocumentWrapper> ingestDocuments = List.of(ingestDocumentWrapper1, ingestDocumentWrapper2);
        final Consumer<List<IngestDocumentWrapper>> hanlder = list -> {};

        exampleSystemIngestProcessor.subBatchExecute(ingestDocuments, hanlder);

        assertEquals(AUTO_ADDED_FIELD_VALUE, ingestDocumentWrapper1.getIngestDocument().getSourceAndMetadata().get(AUTO_ADDED_FIELD_NAME));
        assertEquals(AUTO_ADDED_FIELD_VALUE, ingestDocumentWrapper2.getIngestDocument().getSourceAndMetadata().get(AUTO_ADDED_FIELD_NAME));
    }

    private IngestDocumentWrapper createIngestDocumentWrapper(int slot) {
        final IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        return new IngestDocumentWrapper(slot, 0, ingestDocument, null);
    }

    public void testGetType() {
        assertEquals(ExampleSystemIngestProcessor.TYPE, exampleSystemIngestProcessor.getType());
    }
}
