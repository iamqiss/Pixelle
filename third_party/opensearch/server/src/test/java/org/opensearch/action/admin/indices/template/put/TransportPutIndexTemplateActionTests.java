/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.template.put;

import org.density.action.support.ActionFilter;
import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.AcknowledgedResponse;
import org.density.cluster.ClusterState;
import org.density.cluster.metadata.MetadataIndexTemplateService;
import org.density.common.settings.IndexScopedSettings;
import org.density.common.settings.Settings;
import org.density.core.action.ActionListener;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.index.mapper.MappingTransformerRegistry;
import org.density.test.DensityTestCase;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;
import org.junit.Before;

import java.util.Collections;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportPutIndexTemplateActionTests extends DensityTestCase {
    @Mock
    private TransportService transportService;
    @Mock
    private ActionFilters actionFilters;
    @Mock
    private ThreadPool threadPool;

    @Mock
    private MappingTransformerRegistry mappingTransformerRegistry;
    @Mock
    private MetadataIndexTemplateService indexTemplateService;
    @Mock
    private ActionListener<AcknowledgedResponse> responseListener;
    @Mock
    private ClusterState clusterState;

    private final IndexScopedSettings indexScopedSettings = new IndexScopedSettings(Settings.EMPTY, Collections.emptySet());

    private TransportPutIndexTemplateAction action;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);

        ActionFilter[] emptyActionFilters = new ActionFilter[] {};
        when(actionFilters.filters()).thenReturn(emptyActionFilters);
        action = new TransportPutIndexTemplateAction(
            transportService,
            null, // ClusterService not needed for this test
            threadPool,
            indexTemplateService,
            actionFilters,
            null, // IndexNameExpressionResolver not needed
            indexScopedSettings,
            mappingTransformerRegistry
        );
    }

    public void testClusterManagerOperation_mappingTransformationApplied() {
        // Arrange: Mock the request and response
        PutIndexTemplateRequest request = new PutIndexTemplateRequest("test");
        String originalMappings = "{\"properties\":{\"field\":{\"type\":\"text\"}}}";
        request.mapping(originalMappings, MediaTypeRegistry.JSON);

        // Mock the transformed mapping
        String transformedMappings = "{\"properties\":{\"field\":{\"type\":\"keyword\"}}}";

        // Mock mapping transformer to return transformed mapping
        ArgumentCaptor<ActionListener<String>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        doNothing().when(mappingTransformerRegistry).applyTransformers(anyString(), any(), listenerCaptor.capture());

        // Act: Call the method
        action.clusterManagerOperation(request, clusterState, responseListener);

        // Simulate mapping transformation
        listenerCaptor.getValue().onResponse(transformedMappings);

        // Assert: Verify that the transformed mappings are passed to the template service
        ArgumentCaptor<MetadataIndexTemplateService.PutRequest> putRequestCaptor = ArgumentCaptor.forClass(
            MetadataIndexTemplateService.PutRequest.class
        );
        verify(indexTemplateService).putTemplate(putRequestCaptor.capture(), any());

        // Verify the transformed mappings are set in the PutRequest
        assertEquals(transformedMappings, putRequestCaptor.getValue().getMappings());
    }
}
