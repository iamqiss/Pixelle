/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.grpc;

import org.density.protobufs.BulkRequest;
import org.density.protobufs.BulkRequestBody;
import org.density.protobufs.BulkResponse;
import org.density.protobufs.IndexOperation;
import org.density.protobufs.services.DocumentServiceGrpc;
import org.density.transport.grpc.ssl.NettyGrpcClient;

import io.grpc.ManagedChannel;

/**
 * Integration tests for the DocumentService gRPC service.
 */
public class DocumentServiceIT extends GrpcTransportBaseIT {

    /**
     * Tests the bulk operation via gRPC.
     */
    public void testDocumentServiceBulk() throws Exception {
        // Create a test index
        String indexName = "test-bulk-index";
        createTestIndex(indexName);

        // Create a gRPC client
        try (NettyGrpcClient client = createGrpcClient()) {
            // Create a DocumentService stub
            ManagedChannel channel = client.getChannel();
            DocumentServiceGrpc.DocumentServiceBlockingStub documentStub = DocumentServiceGrpc.newBlockingStub(channel);

            // Create a bulk request with an index operation
            IndexOperation indexOp = IndexOperation.newBuilder().setIndex(indexName).setId("1").build();

            BulkRequestBody requestBody = BulkRequestBody.newBuilder()
                .setIndex(indexOp)
                .setDoc(com.google.protobuf.ByteString.copyFromUtf8(DEFAULT_DOCUMENT_SOURCE))
                .build();

            BulkRequest bulkRequest = BulkRequest.newBuilder().addRequestBody(requestBody).build();

            // Execute the bulk request
            BulkResponse bulkResponse = documentStub.bulk(bulkRequest);

            // Verify the response
            assertNotNull("Bulk response should not be null", bulkResponse);
            assertFalse("Bulk response should not have errors", bulkResponse.getBulkResponseBody().getErrors());
            assertEquals("Bulk response should have one item", 1, bulkResponse.getBulkResponseBody().getItemsCount());

            // Verify the document is searchable
            waitForSearchableDoc(indexName, "1");
        }
    }
}
