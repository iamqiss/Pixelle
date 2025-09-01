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
import org.density.cluster.metadata.ComposableIndexTemplate;
import org.density.cluster.metadata.MetadataIndexTemplateService;
import org.density.cluster.metadata.Template;
import org.density.common.compress.CompressedXContent;
import org.density.core.action.ActionListener;
import org.density.index.mapper.MappingTransformerRegistry;
import org.density.test.DensityTestCase;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;
import org.junit.Before;

import java.io.IOException;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportPutComposableIndexTemplateActionTests extends DensityTestCase {
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

    private TransportPutComposableIndexTemplateAction action;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);

        ActionFilter[] emptyActionFilters = new ActionFilter[] {};
        when(actionFilters.filters()).thenReturn(emptyActionFilters);
        action = new TransportPutComposableIndexTemplateAction(
            transportService,
            null, // ClusterService not needed for this test
            threadPool,
            indexTemplateService,
            actionFilters,
            null, // IndexNameExpressionResolver not needed
            mappingTransformerRegistry
        );
    }

    public void testClusterManagerOperation_mappingTransformationApplied() throws IOException {
        // Arrange: Create a test request and mock dependencies
        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request("test");
        ComposableIndexTemplate indexTemplate = mock(ComposableIndexTemplate.class);
        Template template = mock(Template.class);
        String transformedMapping = "{\"properties\": {\"field\": {\"type\": \"keyword\"}}}";
        when(indexTemplate.template()).thenReturn(template);
        when(template.mappings()).thenReturn(new CompressedXContent("{\"properties\": {\"field\": {\"type\": \"text\"}}}"));
        request.indexTemplate(indexTemplate);

        // Mock mapping transformer to return transformed mapping
        ArgumentCaptor<ActionListener<String>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        doNothing().when(mappingTransformerRegistry).applyTransformers(anyString(), any(), listenerCaptor.capture());

        // Act: Call the method
        action.clusterManagerOperation(request, clusterState, responseListener);

        // Simulate mapping transformation
        listenerCaptor.getValue().onResponse(transformedMapping);

        // Assert: Verify the transformed mappings are set correctly
        verify(template, times(1)).setMappings(new CompressedXContent(transformedMapping));

        // Verify that indexTemplateService.putIndexTemplateV2 is called
        verify(indexTemplateService, times(1)).putIndexTemplateV2(
            eq(request.cause()),
            eq(request.create()),
            eq(request.name()),
            eq(request.clusterManagerNodeTimeout()),
            eq(indexTemplate),
            eq(responseListener)
        );
    }
}
