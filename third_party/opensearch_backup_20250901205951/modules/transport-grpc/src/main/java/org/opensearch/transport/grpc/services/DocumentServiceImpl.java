/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.grpc.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.protobufs.services.DocumentServiceGrpc;
import org.density.transport.client.Client;
import org.density.transport.grpc.listeners.BulkRequestActionListener;
import org.density.transport.grpc.proto.request.document.bulk.BulkRequestProtoUtils;
import org.density.transport.grpc.util.GrpcErrorHandler;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Implementation of the gRPC Document Service.
 */
public class DocumentServiceImpl extends DocumentServiceGrpc.DocumentServiceImplBase {
    private static final Logger logger = LogManager.getLogger(DocumentServiceImpl.class);
    private final Client client;

    /**
     * Creates a new DocumentServiceImpl.
     *
     * @param client Client for executing actions on the local node
     */
    public DocumentServiceImpl(Client client) {
        this.client = client;
    }

    /**
     * Processes a bulk request.
     *
     * @param request The bulk request to process
     * @param responseObserver The observer to send the response back to the client
     */
    @Override
    public void bulk(org.density.protobufs.BulkRequest request, StreamObserver<org.density.protobufs.BulkResponse> responseObserver) {
        try {
            org.density.action.bulk.BulkRequest bulkRequest = BulkRequestProtoUtils.prepareRequest(request);
            BulkRequestActionListener listener = new BulkRequestActionListener(responseObserver);
            client.bulk(bulkRequest, listener);
        } catch (RuntimeException e) {
            logger.error("DocumentServiceImpl failed to process bulk request, request=" + request + ", error=" + e.getMessage());
            StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
            responseObserver.onError(grpcError);
        }
    }
}
