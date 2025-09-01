/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.create;

import org.density.action.support.ActionFilter;
import org.density.action.support.ActionFilters;
import org.density.cluster.ClusterState;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.metadata.MetadataCreateIndexService;
import org.density.core.action.ActionListener;
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

public class TransportCreateIndexActionTests extends DensityTestCase {
    @Mock
    private TransportService transportService;
    @Mock
    private ActionFilters actionFilters;
    @Mock
    private ThreadPool threadPool;
    @Mock
    private MetadataCreateIndexService createIndexService;
    @Mock
    private IndexNameExpressionResolver indexNameExpressionResolver;

    @Mock
    private MappingTransformerRegistry mappingTransformerRegistry;

    @Mock
    private ClusterState clusterState;

    @Mock
    private ActionListener<CreateIndexResponse> responseListener;

    private TransportCreateIndexAction action;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);

        ActionFilter[] emptyActionFilters = new ActionFilter[] {};
        when(actionFilters.filters()).thenReturn(emptyActionFilters);
        action = new TransportCreateIndexAction(
            transportService,
            null, // ClusterService not needed for this test
            threadPool,
            createIndexService,
            actionFilters,
            indexNameExpressionResolver,
            mappingTransformerRegistry
        );
    }

    public void testClusterManagerOperation_usesTransformedMapping() {
        when(indexNameExpressionResolver.resolveDateMathExpression(any())).thenReturn("test-index");

        // Arrange: Create a test request
        final CreateIndexRequest request = new CreateIndexRequest("test-index");
        request.mapping("{\"properties\": {\"field\": {\"type\": \"text\"}}}");

        // Mock transformed mapping result
        final String transformedMapping = "{\"properties\": {\"field\": {\"type\": \"keyword\"}}}";

        // Capture ActionListener passed to applyTransformers
        final ArgumentCaptor<ActionListener<String>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        doNothing().when(mappingTransformerRegistry).applyTransformers(
            anyString(),
            any(),
            listenerCaptor.capture()
        );

        // Act: Call the method
        action.clusterManagerOperation(request, clusterState, responseListener);

        // Simulate transformation completion
        listenerCaptor.getValue().onResponse(transformedMapping);

        // Assert: Capture request sent to createIndexService
        ArgumentCaptor<CreateIndexClusterStateUpdateRequest> updateRequestCaptor =
            ArgumentCaptor.forClass(CreateIndexClusterStateUpdateRequest.class);
        verify(createIndexService, times(1)).createIndex(updateRequestCaptor.capture(), any());

        // Ensure transformed mapping is passed correctly
        CreateIndexClusterStateUpdateRequest capturedRequest = updateRequestCaptor.getValue();
        assertEquals(transformedMapping, capturedRequest.mappings());
    }
}
