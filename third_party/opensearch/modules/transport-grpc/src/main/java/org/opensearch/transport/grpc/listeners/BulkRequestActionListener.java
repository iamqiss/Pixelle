/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.grpc.listeners;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.action.bulk.BulkResponse;
import org.density.core.action.ActionListener;
import org.density.transport.grpc.proto.response.document.bulk.BulkResponseProtoUtils;
import org.density.transport.grpc.util.GrpcErrorHandler;

import java.io.IOException;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Listener for bulk request execution completion, handling successful and failure scenarios.
 */
public class BulkRequestActionListener implements ActionListener<BulkResponse> {
    private static final Logger logger = LogManager.getLogger(BulkRequestActionListener.class);
    private final StreamObserver<org.density.protobufs.BulkResponse> responseObserver;

    /**
     * Creates a new BulkRequestActionListener.
     *
     * @param responseObserver The gRPC stream observer to send the response back to the client
     */
    public BulkRequestActionListener(StreamObserver<org.density.protobufs.BulkResponse> responseObserver) {
        super();
        this.responseObserver = responseObserver;
    }

    /**
     * Handles successful bulk request execution.
     * Converts the Density internal response to protobuf format and sends it to the client.
     *
     * @param response The bulk response from Density
     */
    @Override
    public void onResponse(org.density.action.bulk.BulkResponse response) {
        // Bulk execution succeeded. Convert the density internal response to protobuf
        try {
            org.density.protobufs.BulkResponse protoResponse = BulkResponseProtoUtils.toProto(response);
            responseObserver.onNext(protoResponse);
            responseObserver.onCompleted();
        } catch (RuntimeException | IOException e) {
            logger.error("Failed to convert bulk response to protobuf: " + e.getMessage());
            StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
            responseObserver.onError(grpcError);
        }
    }

    /**
     * Handles bulk request execution failures.
     * Converts the exception to an appropriate gRPC error and sends it to the client.
     *
     * @param e The exception that occurred during execution
     */
    @Override
    public void onFailure(Exception e) {
        logger.error("BulkRequestActionListener failed to process bulk request: " + e.getMessage());
        StatusRuntimeException grpcError = GrpcErrorHandler.convertToGrpcError(e);
        responseObserver.onError(grpcError);
    }
}
