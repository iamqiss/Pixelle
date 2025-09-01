/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm;

import org.density.action.ActionRequest;
import org.density.action.support.ActionFilter;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.node.DiscoveryNodes;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Module;
import org.density.common.network.NetworkService;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.IndexScopedSettings;
import org.density.common.settings.Setting;
import org.density.common.settings.Settings;
import org.density.common.settings.SettingsFilter;
import org.density.core.action.ActionResponse;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.discovery.SeedHostsProvider;
import org.density.env.Environment;
import org.density.env.NodeEnvironment;
import org.density.indices.SystemIndexDescriptor;
import org.density.plugin.wlm.action.CreateWorkloadGroupAction;
import org.density.plugin.wlm.action.DeleteWorkloadGroupAction;
import org.density.plugin.wlm.action.GetWorkloadGroupAction;
import org.density.plugin.wlm.action.TransportCreateWorkloadGroupAction;
import org.density.plugin.wlm.action.TransportDeleteWorkloadGroupAction;
import org.density.plugin.wlm.action.TransportGetWorkloadGroupAction;
import org.density.plugin.wlm.action.TransportUpdateWorkloadGroupAction;
import org.density.plugin.wlm.action.UpdateWorkloadGroupAction;
import org.density.plugin.wlm.rest.RestCreateWorkloadGroupAction;
import org.density.plugin.wlm.rest.RestDeleteWorkloadGroupAction;
import org.density.plugin.wlm.rest.RestGetWorkloadGroupAction;
import org.density.plugin.wlm.rest.RestUpdateWorkloadGroupAction;
import org.density.plugin.wlm.rule.WorkloadGroupFeatureType;
import org.density.plugin.wlm.rule.WorkloadGroupFeatureValueValidator;
import org.density.plugin.wlm.rule.WorkloadGroupRuleRoutingService;
import org.density.plugin.wlm.rule.sync.RefreshBasedSyncMechanism;
import org.density.plugin.wlm.rule.sync.detect.RuleEventClassifier;
import org.density.plugin.wlm.service.WorkloadGroupPersistenceService;
import org.density.plugins.ActionPlugin;
import org.density.plugins.DiscoveryPlugin;
import org.density.plugins.Plugin;
import org.density.plugins.SystemIndexPlugin;
import org.density.repositories.RepositoriesService;
import org.density.rest.RestController;
import org.density.rest.RestHandler;
import org.density.rule.InMemoryRuleProcessingService;
import org.density.rule.RuleEntityParser;
import org.density.rule.RulePersistenceService;
import org.density.rule.RuleRoutingService;
import org.density.rule.autotagging.FeatureType;
import org.density.rule.service.IndexStoredRulePersistenceService;
import org.density.rule.spi.RuleFrameworkExtension;
import org.density.rule.storage.AttributeValueStoreFactory;
import org.density.rule.storage.DefaultAttributeValueStore;
import org.density.rule.storage.IndexBasedRuleQueryMapper;
import org.density.rule.storage.XContentRuleParser;
import org.density.script.ScriptService;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;
import org.density.transport.client.Client;
import org.density.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.density.rule.service.IndexStoredRulePersistenceService.MAX_WLM_RULES_SETTING;

/**
 * Plugin class for WorkloadManagement
 */
public class WorkloadManagementPlugin extends Plugin implements ActionPlugin, SystemIndexPlugin, DiscoveryPlugin, RuleFrameworkExtension {

    /**
     * The name of the index where rules are stored.
     */
    public static final String INDEX_NAME = ".wlm_rules";
    /**
     * The maximum number of rules allowed per GET request.
     */
    public static final int MAX_RULES_PER_PAGE = 50;
    private static FeatureType featureType;
    private static RulePersistenceService rulePersistenceService;
    private static RuleRoutingService ruleRoutingService;
    private WlmClusterSettingValuesProvider wlmClusterSettingValuesProvider;
    private AutoTaggingActionFilter autoTaggingActionFilter;

    /**
     * Default constructor
     */
    public WorkloadManagementPlugin() {}

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        wlmClusterSettingValuesProvider = new WlmClusterSettingValuesProvider(
            clusterService.getSettings(),
            clusterService.getClusterSettings()
        );
        featureType = new WorkloadGroupFeatureType(new WorkloadGroupFeatureValueValidator(clusterService));
        RuleEntityParser parser = new XContentRuleParser(featureType);
        AttributeValueStoreFactory attributeValueStoreFactory = new AttributeValueStoreFactory(
            featureType,
            DefaultAttributeValueStore::new
        );
        InMemoryRuleProcessingService ruleProcessingService = new InMemoryRuleProcessingService(attributeValueStoreFactory);
        rulePersistenceService = new IndexStoredRulePersistenceService(
            INDEX_NAME,
            client,
            clusterService,
            MAX_RULES_PER_PAGE,
            parser,
            new IndexBasedRuleQueryMapper()
        );
        ruleRoutingService = new WorkloadGroupRuleRoutingService(client, clusterService);

        RefreshBasedSyncMechanism refreshMechanism = new RefreshBasedSyncMechanism(
            threadPool,
            clusterService.getSettings(),
            featureType,
            rulePersistenceService,
            new RuleEventClassifier(Collections.emptySet(), ruleProcessingService),
            wlmClusterSettingValuesProvider
        );

        autoTaggingActionFilter = new AutoTaggingActionFilter(ruleProcessingService, threadPool);
        return List.of(refreshMechanism);
    }

    @Override
    public Map<String, Supplier<SeedHostsProvider>> getSeedHostProviders(TransportService transportService, NetworkService networkService) {
        ((WorkloadGroupRuleRoutingService) ruleRoutingService).setTransportService(transportService);
        return Collections.emptyMap();
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        return List.of(autoTaggingActionFilter);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionPlugin.ActionHandler<>(CreateWorkloadGroupAction.INSTANCE, TransportCreateWorkloadGroupAction.class),
            new ActionPlugin.ActionHandler<>(GetWorkloadGroupAction.INSTANCE, TransportGetWorkloadGroupAction.class),
            new ActionPlugin.ActionHandler<>(DeleteWorkloadGroupAction.INSTANCE, TransportDeleteWorkloadGroupAction.class),
            new ActionPlugin.ActionHandler<>(UpdateWorkloadGroupAction.INSTANCE, TransportUpdateWorkloadGroupAction.class)
        );
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(new SystemIndexDescriptor(INDEX_NAME, "System index used for storing workload_group rules"));
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(
            new RestCreateWorkloadGroupAction(wlmClusterSettingValuesProvider),
            new RestGetWorkloadGroupAction(),
            new RestDeleteWorkloadGroupAction(wlmClusterSettingValuesProvider),
            new RestUpdateWorkloadGroupAction(wlmClusterSettingValuesProvider)
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            WorkloadGroupPersistenceService.MAX_QUERY_GROUP_COUNT,
            RefreshBasedSyncMechanism.RULE_SYNC_REFRESH_INTERVAL_SETTING,
            MAX_WLM_RULES_SETTING
        );
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return List.of(new WorkloadManagementPluginModule());
    }

    @Override
    public Supplier<RulePersistenceService> getRulePersistenceServiceSupplier() {
        return () -> rulePersistenceService;
    }

    @Override
    public Supplier<RuleRoutingService> getRuleRoutingServiceSupplier() {
        return () -> ruleRoutingService;
    }

    @Override
    public Supplier<FeatureType> getFeatureTypeSupplier() {
        return () -> featureType;
    }
}
