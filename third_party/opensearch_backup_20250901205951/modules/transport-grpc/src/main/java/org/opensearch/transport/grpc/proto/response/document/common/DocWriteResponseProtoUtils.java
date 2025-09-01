/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.grpc.proto.response.document.common;

import org.density.action.DocWriteResponse;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.protobufs.NullValue;
import org.density.protobufs.ResponseItem;
import org.density.protobufs.ShardInfo;

import java.io.IOException;

/**
 * Utility class for converting DocWriteResponse objects to Protocol Buffers.
 * This class handles the conversion of document write operation responses (index, create, update, delete)
 * to their Protocol Buffer representation.
 */
public class DocWriteResponseProtoUtils {

    private DocWriteResponseProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a DocWriteResponse to its Protocol Buffer representation.
     * This method is equivalent to the {@link DocWriteResponse#innerToXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param response The DocWriteResponse to convert
     * @return A ResponseItem.Builder with the DocWriteResponse data
     *
     */
    public static ResponseItem.Builder toProto(DocWriteResponse response) throws IOException {
        ResponseItem.Builder responseItem = ResponseItem.newBuilder();

        // Set the index name
        responseItem.setIndex(response.getIndex());

        // Handle document ID (can be null in some cases)
        if (response.getId().isEmpty()) {
            responseItem.setId(ResponseItem.Id.newBuilder().setNullValue(NullValue.NULL_VALUE_NULL).build());
        } else {
            responseItem.setId(ResponseItem.Id.newBuilder().setString(response.getId()).build());
        }

        // Set document version
        responseItem.setVersion(response.getVersion());

        // Set operation result (CREATED, UPDATED, DELETED, NOT_FOUND, NOOP)
        responseItem.setResult(response.getResult().getLowercase());

        // Set forced refresh flag if applicable
        if (response.forcedRefresh()) {
            responseItem.setForcedRefresh(true);
        }
        // Handle shard information
        ShardInfo shardInfo = ShardInfoProtoUtils.toProto(response.getShardInfo());
        responseItem.setShards(shardInfo);

        // Set sequence number and primary term if available
        if (response.getSeqNo() >= 0) {
            responseItem.setSeqNo(response.getSeqNo());
            responseItem.setPrimaryTerm(response.getPrimaryTerm());
        }

        return responseItem;
    }
}
