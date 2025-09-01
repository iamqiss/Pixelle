/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.rule.sync;

import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.core.action.ActionListener;
import org.density.index.IndexNotFoundException;
import org.density.plugin.wlm.AutoTaggingActionFilterTests;
import org.density.plugin.wlm.WlmClusterSettingValuesProvider;
import org.density.plugin.wlm.WorkloadManagementPlugin;
import org.density.plugin.wlm.rule.sync.detect.RuleEventClassifier;
import org.density.rule.InMemoryRuleProcessingService;
import org.density.rule.RulePersistenceService;
import org.density.rule.action.GetRuleRequest;
import org.density.rule.action.GetRuleResponse;
import org.density.rule.autotagging.Attribute;
import org.density.rule.autotagging.FeatureType;
import org.density.rule.autotagging.Rule;
import org.density.rule.storage.AttributeValueStoreFactory;
import org.density.rule.storage.DefaultAttributeValueStore;
import org.density.test.DensityTestCase;
import org.density.threadpool.Scheduler;
import org.density.threadpool.ThreadPool;
import org.density.transport.client.Client;
import org.density.wlm.WorkloadManagementSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.density.plugin.wlm.rule.sync.detect.RuleEventClassifierTests.getRandomRule;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RefreshBasedSyncMechanismTests extends DensityTestCase {
    RefreshBasedSyncMechanism sut;

    Client mockClient;
    InMemoryRuleProcessingService ruleProcessingService;
    RulePersistenceService rulePersistenceService;
    AttributeValueStoreFactory attributeValueStoreFactory;
    ThreadPool mockThreadPool;
    Scheduler.Cancellable scheduledFuture;
    RuleEventClassifier ruleEventClassifier;
    FeatureType featureType;
    WlmClusterSettingValuesProvider nonPluginSettingValuesProvider;
    ClusterSettings clusterSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        try (WorkloadManagementPlugin plugin = new WorkloadManagementPlugin()) {
            Settings settings = Settings.builder()
                .put(RefreshBasedSyncMechanism.RULE_SYNC_REFRESH_INTERVAL_SETTING_NAME, 1000)
                .put(WorkloadManagementSettings.WLM_MODE_SETTING_NAME, "enabled")
                .build();
            clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(plugin.getSettings()));
            clusterSettings.registerSetting(WorkloadManagementSettings.WLM_MODE_SETTING);
            featureType = mock(FeatureType.class);
            mockThreadPool = mock(ThreadPool.class);
            ruleProcessingService = mock(InMemoryRuleProcessingService.class);
            rulePersistenceService = mock(RulePersistenceService.class);
            ruleEventClassifier = new RuleEventClassifier(Collections.emptySet(), ruleProcessingService);
            attributeValueStoreFactory = new AttributeValueStoreFactory(featureType, DefaultAttributeValueStore::new);
            nonPluginSettingValuesProvider = new WlmClusterSettingValuesProvider(settings, clusterSettings);
            mockClient = mock(Client.class);
            scheduledFuture = mock(Scheduler.Cancellable.class);
            when(mockThreadPool.scheduleWithFixedDelay(any(), any(), any())).thenReturn(scheduledFuture);

            sut = new RefreshBasedSyncMechanism(
                mockThreadPool,
                settings,
                featureType,
                rulePersistenceService,
                ruleEventClassifier,
                nonPluginSettingValuesProvider
            );
        }
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testDoStart() {
        sut.doStart();
        verify(mockThreadPool, times(1)).scheduleWithFixedDelay(any(), any(), any());
    }

    public void testDoStop() {
        sut.doStart();
        sut.doStop();
        verify(scheduledFuture, times(1)).cancel();
    }

    public void testDoClose() throws IOException {
        sut.doStart();
        sut.doClose();
        verify(scheduledFuture, times(1)).cancel();
    }

    /**
     * Tests the behavior of doRun when the WLM mode is disabled.
     * This test verifies that the method does not perform any operations
     * when the WLM mode is set to DISABLED.
     */
    @SuppressWarnings("unchecked")
    public void testDoRunWhenWLM_isDisabled() {
        Settings disabledSettings = Settings.builder()
            .put(RefreshBasedSyncMechanism.RULE_SYNC_REFRESH_INTERVAL_SETTING_NAME, 1000)
            .put(WorkloadManagementSettings.WLM_MODE_SETTING_NAME, "disabled")
            .build();
        WlmClusterSettingValuesProvider disabledWlmModeProvider = new WlmClusterSettingValuesProvider(disabledSettings, clusterSettings);
        sut = new RefreshBasedSyncMechanism(
            mockThreadPool,
            disabledSettings,
            featureType,
            rulePersistenceService,
            ruleEventClassifier,
            disabledWlmModeProvider
        );
        sut.doRun();
        verify(rulePersistenceService, times(0)).getRule(any(GetRuleRequest.class), any(ActionListener.class));
    }

    /**
     * Tests the behavior of doRun when the search operation fails.
     * This test verifies that the method handles the failure case correctly
     * by logging the failure without throwing an exception.
     */
    @SuppressWarnings("unchecked")
    public void testDoRunSearchFailure() {
        doAnswer(invocation -> {
            ActionListener<GetRuleResponse> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException("Search failed"));
            return null;
        }).when(rulePersistenceService).getRule(any(GetRuleRequest.class), any(ActionListener.class));

        sut.doRun();

        verify(rulePersistenceService, times(1)).getRule(any(GetRuleRequest.class), any(ActionListener.class));
        verify(ruleProcessingService, times(0)).add(any(Rule.class));
        verify(ruleProcessingService, times(0)).remove(any(Rule.class));
    }

    /**
     * Test case for the doRun() method.
     * This test verifies that the doRun() method clears rules from the in-memory service
     * and attempts to refresh rules by executing a search request.
     */
    @SuppressWarnings("unchecked")
    public void test_doRun_RefreshesRules() {
        GetRuleResponse getRuleResponse = mock(GetRuleResponse.class);
        Map<Attribute, Set<String>> attributeSetMap = Map.of(AutoTaggingActionFilterTests.TestAttribute.TEST_ATTRIBUTE, Set.of("test"));
        Rule rule = Rule.builder()
            .description("test description")
            .attributeMap(attributeSetMap)
            .featureType(AutoTaggingActionFilterTests.WLMFeatureType.WLM)
            .featureValue("test_value")
            .updatedAt("2025-05-27T08:58:57.558Z")
            .id("test_id")
            .build();

        when(getRuleResponse.getRules()).thenReturn(List.of(rule));
        doAnswer(invocation -> {
            ActionListener<GetRuleResponse> listener = invocation.getArgument(1);
            listener.onResponse(getRuleResponse);
            return null;
        }).when(rulePersistenceService).getRule(any(GetRuleRequest.class), any(ActionListener.class));

        sut.doRun();

        verify(ruleProcessingService, times(1)).add(rule);
    }

    @SuppressWarnings("unchecked")
    public void test_doRun_RefreshesRulesAndCheckInMemoryView() {
        GetRuleResponse getRuleResponse = mock(GetRuleResponse.class);
        List<Rule> existingRules = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final String randomRuleId = randomAlphaOfLength(5);
            existingRules.add(getRandomRule(randomRuleId));
        }

        when(getRuleResponse.getRules()).thenReturn(existingRules);
        doAnswer(invocation -> {
            ActionListener<GetRuleResponse> listener = invocation.getArgument(1);
            listener.onResponse(getRuleResponse);
            return null;
        }).when(rulePersistenceService).getRule(any(GetRuleRequest.class), any(ActionListener.class));

        // marks the first run of service
        sut.doRun();

        Set<Rule> previousRules = new HashSet<>(existingRules);

        Set<Rule> newRules = new HashSet<>();

        int deletionEventCount = 10;
        // Mark some deletions
        for (Rule rule : existingRules) {
            if (randomBoolean()) {
                deletionEventCount--;
                newRules.add(rule);
            }
        }

        // add new rule
        newRules.add(getRandomRule("10"));

        int updateEventCount = 0;
        // Update some rules
        for (Rule rule : previousRules) {
            if (randomBoolean() && !newRules.contains(rule)) {
                updateEventCount++;
                // since we are updating a new existing rule but we have marked it for deletion above hence decrement it
                deletionEventCount--;
                newRules.add(getRandomRule(rule.getId()));
            }
        }

        when(getRuleResponse.getRules()).thenReturn(new ArrayList<>(newRules));
        doAnswer(invocation -> {
            ActionListener<GetRuleResponse> listener = invocation.getArgument(1);
            listener.onResponse(getRuleResponse);
            return null;
        }).when(rulePersistenceService).getRule(any(GetRuleRequest.class), any(ActionListener.class));

        sut.doRun();

        verify(ruleProcessingService, times(deletionEventCount + updateEventCount)).remove(any(Rule.class));
        // Here 1 is due to add in the second run and 10 for adding 10 rules as part of first run
        verify(ruleProcessingService, times(updateEventCount + 1 + 10)).add(any(Rule.class));
    }

    @SuppressWarnings("unchecked")
    public void testDoRunIgnoresIndexNotFoundException() {
        doAnswer(invocation -> {
            ActionListener<GetRuleResponse> listener = invocation.getArgument(1);
            listener.onFailure(new IndexNotFoundException("rules index not found"));
            return null;
        }).when(rulePersistenceService).getRule(any(GetRuleRequest.class), any(ActionListener.class));
        sut.doRun();
        verify(rulePersistenceService, times(1)).getRule(any(GetRuleRequest.class), any(ActionListener.class));
        verify(ruleProcessingService, times(0)).add(any(Rule.class));
        verify(ruleProcessingService, times(0)).remove(any(Rule.class));
    }
}
