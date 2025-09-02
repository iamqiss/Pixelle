/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.grpc.proto.request.search.sort;

import org.density.search.sort.SortOrder;
import org.density.test.DensityTestCase;

public class SortOrderProtoUtilsTests extends DensityTestCase {

    public void testFromProtoScoreSortAsc() {
        // Test ASC order
        SortOrder sortOrder = SortOrderProtoUtils.fromProto(org.density.protobufs.SortOrder.SORT_ORDER_ASC);
        assertEquals("Sort order should be ASC", SortOrder.ASC, sortOrder);
    }

    public void testFromProtoScoreSortDesc() {
        // Test DESC order
        SortOrder sortOrder = SortOrderProtoUtils.fromProto(org.density.protobufs.SortOrder.SORT_ORDER_DESC);
        assertEquals("Sort order should be DESC", SortOrder.DESC, sortOrder);
    }

    public void testFromProtoScoreSortUnspecified() {
        // Test UNSPECIFIED order (should throw exception)
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> SortOrderProtoUtils.fromProto(org.density.protobufs.SortOrder.SORT_ORDER_UNSPECIFIED)
        );
        assertTrue(
            "Exception message should mention 'Must provide oneof sort combinations'",
            exception.getMessage().contains("Must provide oneof sort combinations")
        );
    }

    public void testFromProtoGeoDistanceSortAsc() {
        // Test ASC order
        SortOrder sortOrder = SortOrderProtoUtils.fromProto(org.density.protobufs.SortOrder.SORT_ORDER_ASC);
        assertEquals("Sort order should be ASC", SortOrder.ASC, sortOrder);
    }

    public void testFromProtoGeoDistanceSortDesc() {
        // Test DESC order
        SortOrder sortOrder = SortOrderProtoUtils.fromProto(org.density.protobufs.SortOrder.SORT_ORDER_DESC);
        assertEquals("Sort order should be DESC", SortOrder.DESC, sortOrder);
    }

    public void testFromProtoGeoDistanceSortUnspecified() {
        // Test UNSPECIFIED order (should throw exception)
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> SortOrderProtoUtils.fromProto(org.density.protobufs.SortOrder.SORT_ORDER_UNSPECIFIED)
        );
        assertTrue(
            "Exception message should mention 'Must provide oneof sort combinations'",
            exception.getMessage().contains("Must provide oneof sort combinations")
        );
    }

    public void testFromProtoScriptSortAsc() {
        // Test ASC order
        SortOrder sortOrder = SortOrderProtoUtils.fromProto(org.density.protobufs.SortOrder.SORT_ORDER_ASC);
        assertEquals("Sort order should be ASC", SortOrder.ASC, sortOrder);
    }

    public void testFromProtoScriptSortDesc() {
        // Test DESC order
        SortOrder sortOrder = SortOrderProtoUtils.fromProto(org.density.protobufs.SortOrder.SORT_ORDER_DESC);
        assertEquals("Sort order should be DESC", SortOrder.DESC, sortOrder);
    }

    public void testFromProtoScriptSortUnspecified() {
        // Test UNSPECIFIED order (should throw exception)
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> SortOrderProtoUtils.fromProto(org.density.protobufs.SortOrder.SORT_ORDER_UNSPECIFIED)
        );
        assertTrue(
            "Exception message should mention 'Must provide oneof sort combinations'",
            exception.getMessage().contains("Must provide oneof sort combinations")
        );
    }
}
