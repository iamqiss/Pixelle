/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule;

import org.density.action.ActionRequest;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.node.DiscoveryNodes;
import org.density.common.inject.Module;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.IndexScopedSettings;
import org.density.common.settings.Settings;
import org.density.common.settings.SettingsFilter;
import org.density.core.action.ActionResponse;
import org.density.plugins.ActionPlugin;
import org.density.plugins.ExtensiblePlugin;
import org.density.plugins.Plugin;
import org.density.rest.RestController;
import org.density.rest.RestHandler;
import org.density.rule.action.CreateRuleAction;
import org.density.rule.action.DeleteRuleAction;
import org.density.rule.action.GetRuleAction;
import org.density.rule.action.TransportCreateRuleAction;
import org.density.rule.action.TransportDeleteRuleAction;
import org.density.rule.action.TransportGetRuleAction;
import org.density.rule.action.TransportUpdateRuleAction;
import org.density.rule.action.UpdateRuleAction;
import org.density.rule.autotagging.AutoTaggingRegistry;
import org.density.rule.autotagging.FeatureType;
import org.density.rule.rest.RestCreateRuleAction;
import org.density.rule.rest.RestDeleteRuleAction;
import org.density.rule.rest.RestGetRuleAction;
import org.density.rule.rest.RestUpdateRuleAction;
import org.density.rule.spi.RuleFrameworkExtension;
import org.density.threadpool.ExecutorBuilder;
import org.density.threadpool.FixedExecutorBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * This plugin provides the central APIs which can provide CRUD support to all consumers of Rule framework
 * Plugins that define custom rule logic must implement {@link RuleFrameworkExtension}, which ensures
 * their feature types and persistence services are automatically registered and available to the Rule Framework.
 */
public class RuleFrameworkPlugin extends Plugin implements ExtensiblePlugin, ActionPlugin {

    /**
     * The name of the thread pool dedicated to rule execution.
     */
    public static final String RULE_THREAD_POOL_NAME = "rule_serial_executor";
    /**
     * The number of threads allocated in the rule execution thread pool. This is set to 1 to ensure serial execution.
     */
    public static final int RULE_THREAD_COUNT = 1;
    /**
     * The maximum number of tasks that can be queued in the rule execution thread pool.
     */
    public static final int RULE_QUEUE_SIZE = 100;

    /**
     * constructor for RuleFrameworkPlugin
     */
    public RuleFrameworkPlugin() {}

    private final RulePersistenceServiceRegistry rulePersistenceServiceRegistry = new RulePersistenceServiceRegistry();
    private final RuleRoutingServiceRegistry ruleRoutingServiceRegistry = new RuleRoutingServiceRegistry();
    private final List<RuleFrameworkExtension> ruleFrameworkExtensions = new ArrayList<>();

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        // We are consuming the extensions at this place to ensure that the RulePersistenceService is initialised
        ruleFrameworkExtensions.forEach(this::consumeFrameworkExtension);
        return List.of(
            new ActionPlugin.ActionHandler<>(GetRuleAction.INSTANCE, TransportGetRuleAction.class),
            new ActionPlugin.ActionHandler<>(DeleteRuleAction.INSTANCE, TransportDeleteRuleAction.class),
            new ActionPlugin.ActionHandler<>(CreateRuleAction.INSTANCE, TransportCreateRuleAction.class),
            new ActionPlugin.ActionHandler<>(UpdateRuleAction.INSTANCE, TransportUpdateRuleAction.class)
        );
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
        return List.of(new RestGetRuleAction(), new RestDeleteRuleAction(), new RestCreateRuleAction(), new RestUpdateRuleAction());
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(new FixedExecutorBuilder(settings, RULE_THREAD_POOL_NAME, RULE_THREAD_COUNT, RULE_QUEUE_SIZE, "rule-threadpool"));
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return List.of(b -> {
            b.bind(RulePersistenceServiceRegistry.class).toInstance(rulePersistenceServiceRegistry);
            b.bind(RuleRoutingServiceRegistry.class).toInstance(ruleRoutingServiceRegistry);
        });
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        ruleFrameworkExtensions.addAll(loader.loadExtensions(RuleFrameworkExtension.class));
    }

    private void consumeFrameworkExtension(RuleFrameworkExtension ruleFrameworkExtension) {
        FeatureType featureType = ruleFrameworkExtension.getFeatureTypeSupplier().get();
        AutoTaggingRegistry.registerFeatureType(featureType);
        rulePersistenceServiceRegistry.register(featureType, ruleFrameworkExtension.getRulePersistenceServiceSupplier().get());
        ruleRoutingServiceRegistry.register(featureType, ruleFrameworkExtension.getRuleRoutingServiceSupplier().get());

    }
}
