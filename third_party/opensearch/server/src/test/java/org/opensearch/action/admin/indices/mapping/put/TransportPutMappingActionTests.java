/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.mapping.put;

import org.density.action.RequestValidators;
import org.density.action.support.ActionFilter;
import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.AcknowledgedResponse;
import org.density.cluster.ClusterState;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.metadata.MetadataMappingService;
import org.density.core.action.ActionListener;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.index.mapper.MappingTransformerRegistry;
import org.density.test.DensityTestCase;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;
import org.junit.Before;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportPutMappingActionTests extends DensityTestCase {
    @Mock
    private TransportService transportService;
    @Mock
    private ActionFilters actionFilters;
    @Mock
    private ThreadPool threadPool;
    @Mock
    private IndexNameExpressionResolver indexNameExpressionResolver;

    @Mock
    private MetadataMappingService metadataMappingService;

    @Mock
    private MappingTransformerRegistry mappingTransformerRegistry;

    @Mock
    private RequestValidators<PutMappingRequest> requestValidators;

    @Mock
    private ClusterState clusterState;

    @Mock
    private ActionListener<AcknowledgedResponse> responseListener;

    private TransportPutMappingAction action;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);

        ActionFilter[] emptyActionFilters = new ActionFilter[] {};
        when(actionFilters.filters()).thenReturn(emptyActionFilters);
        action = new TransportPutMappingAction(
            transportService,
            null, // ClusterService not needed for this test
            threadPool,
            metadataMappingService,
            actionFilters,
            indexNameExpressionResolver,
            requestValidators,
            mappingTransformerRegistry
        );
    }

    public void testClusterManagerOperation_transformedMappingUsed() {
        // Arrange: Create a test request
        final PutMappingRequest request = new PutMappingRequest("index");
        final String originalMapping = "{\"properties\": {\"field\": {\"type\": \"text\"}}}";
        request.source(originalMapping, MediaTypeRegistry.JSON);

        String transformedMapping = "{\"properties\": {\"field\": {\"type\": \"keyword\"}}}";

        // Mock the transformer registry to return the transformed mapping
        ArgumentCaptor<ActionListener<String>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        doNothing().when(mappingTransformerRegistry).applyTransformers(anyString(), any(), listenerCaptor.capture());

        // Act: Call the method
        action.clusterManagerOperation(request, clusterState, responseListener);

        // Simulate transformation completion
        listenerCaptor.getValue().onResponse(transformedMapping);

        // Assert: Verify the transformed mapping is passed to metadataMappingService
        ArgumentCaptor<PutMappingClusterStateUpdateRequest> updateRequestCaptor = ArgumentCaptor.forClass(
            PutMappingClusterStateUpdateRequest.class
        );
        verify(metadataMappingService, times(1)).putMapping(updateRequestCaptor.capture(), any());

        // Ensure the transformed mapping is used correctly
        PutMappingClusterStateUpdateRequest capturedRequest = updateRequestCaptor.getValue();
        assertEquals(transformedMapping, capturedRequest.source());
    }
}
