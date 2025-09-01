/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm;

import org.density.action.ActionRequest;
import org.density.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.density.action.search.SearchRequest;
import org.density.action.support.ActionFilterChain;
import org.density.common.util.concurrent.ThreadContext;
import org.density.core.action.ActionListener;
import org.density.core.action.ActionResponse;
import org.density.core.common.io.stream.StreamOutput;
import org.density.rule.InMemoryRuleProcessingService;
import org.density.rule.autotagging.Attribute;
import org.density.rule.autotagging.FeatureType;
import org.density.rule.storage.AttributeValueStoreFactory;
import org.density.rule.storage.DefaultAttributeValueStore;
import org.density.tasks.Task;
import org.density.test.DensityTestCase;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.wlm.WorkloadGroupTask;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AutoTaggingActionFilterTests extends DensityTestCase {

    AutoTaggingActionFilter autoTaggingActionFilter;
    InMemoryRuleProcessingService ruleProcessingService;
    ThreadPool threadPool;

    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("AutoTaggingActionFilterTests");
        AttributeValueStoreFactory attributeValueStoreFactory = new AttributeValueStoreFactory(
            WLMFeatureType.WLM,
            DefaultAttributeValueStore::new
        );
        ruleProcessingService = spy(new InMemoryRuleProcessingService(attributeValueStoreFactory));
        autoTaggingActionFilter = new AutoTaggingActionFilter(ruleProcessingService, threadPool);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testOrder() {
        assertEquals(Integer.MAX_VALUE, autoTaggingActionFilter.order());
    }

    public void testApplyForValidRequest() {
        SearchRequest request = mock(SearchRequest.class);
        ActionFilterChain<ActionRequest, ActionResponse> mockFilterChain = mock(TestActionFilterChain.class);
        when(request.indices()).thenReturn(new String[] { "foo" });
        try (ThreadContext.StoredContext context = threadPool.getThreadContext().stashContext()) {
            when(ruleProcessingService.evaluateLabel(anyList())).thenReturn(Optional.of("TestQG_ID"));
            autoTaggingActionFilter.apply(mock(Task.class), "Test", request, null, mockFilterChain);

            assertEquals("TestQG_ID", threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER));
            verify(ruleProcessingService, times(1)).evaluateLabel(anyList());
        }
    }

    public void testApplyForInValidRequest() {
        ActionFilterChain<ActionRequest, ActionResponse> mockFilterChain = mock(TestActionFilterChain.class);
        CancelTasksRequest request = new CancelTasksRequest();
        autoTaggingActionFilter.apply(mock(Task.class), "Test", request, null, mockFilterChain);

        verify(ruleProcessingService, times(0)).evaluateLabel(anyList());
    }

    public enum WLMFeatureType implements FeatureType {
        WLM;

        @Override
        public String getName() {
            return "";
        }

        @Override
        public Map<String, Attribute> getAllowedAttributesRegistry() {
            return Map.of("test_attribute", TestAttribute.TEST_ATTRIBUTE);
        }
    }

    public enum TestAttribute implements Attribute {
        TEST_ATTRIBUTE("test_attribute"),
        INVALID_ATTRIBUTE("invalid_attribute");

        private final String name;

        TestAttribute(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public void validateAttribute() {}

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }

    private static class TestActionFilterChain implements ActionFilterChain<ActionRequest, ActionResponse> {
        @Override
        public void proceed(Task task, String action, ActionRequest request, ActionListener<ActionResponse> listener) {

        }
    }
}
