/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.grpc.listeners;

import org.density.action.search.SearchResponse;
import org.density.action.search.SearchResponseSections;
import org.density.action.search.ShardSearchFailure;
import org.density.search.SearchHits;
import org.density.test.DensityTestCase;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SearchRequestActionListenerTests extends DensityTestCase {

    @Mock
    private StreamObserver<org.density.protobufs.SearchResponse> responseObserver;

    private SearchRequestActionListener listener;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        listener = new SearchRequestActionListener(responseObserver);
    }

    public void testOnResponse() {

        // Create a SearchResponse
        SearchResponse mockSearchResponse = new SearchResponse(
            new SearchResponseSections(SearchHits.empty(), null, null, false, false, null, 1),
            randomAlphaOfLengthBetween(5, 10),
            5,
            5,
            0,
            100,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );

        // Call the method under test
        listener.onResponse(mockSearchResponse);

        // Verify that onNext and onCompleted were called
        verify(responseObserver, times(1)).onNext(any(org.density.protobufs.SearchResponse.class));
        verify(responseObserver, times(1)).onCompleted();
    }

    public void testOnFailure() {
        // Create a mock StreamObserver
        @SuppressWarnings("unchecked")
        StreamObserver<org.density.protobufs.SearchResponse> mockResponseObserver = mock(StreamObserver.class);

        // Create a SearchRequestActionListener
        SearchRequestActionListener listener = new SearchRequestActionListener(mockResponseObserver);

        // Create an exception
        Exception exception = new Exception("Test exception");

        // Call the method under test
        listener.onFailure(exception);

        // Verify that onError was called with a StatusRuntimeException
        verify(mockResponseObserver, times(1)).onError(any(StatusRuntimeException.class));
    }
}
