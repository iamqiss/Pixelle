/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.painless;

import org.density.action.ActionRequest;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.node.DiscoveryNodes;
import org.density.cluster.service.ClusterService;
import org.density.common.SetOnce;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.IndexScopedSettings;
import org.density.common.settings.Setting;
import org.density.common.settings.Settings;
import org.density.common.settings.SettingsFilter;
import org.density.core.action.ActionResponse;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.env.Environment;
import org.density.env.NodeEnvironment;
import org.density.painless.action.PainlessContextAction;
import org.density.painless.action.PainlessExecuteAction;
import org.density.painless.spi.Allowlist;
import org.density.painless.spi.AllowlistLoader;
import org.density.painless.spi.PainlessExtension;
import org.density.plugins.ActionPlugin;
import org.density.plugins.ExtensiblePlugin;
import org.density.plugins.Plugin;
import org.density.plugins.ScriptPlugin;
import org.density.repositories.RepositoriesService;
import org.density.rest.RestController;
import org.density.rest.RestHandler;
import org.density.script.DerivedFieldScript;
import org.density.script.IngestScript;
import org.density.script.ScoreScript;
import org.density.script.ScriptContext;
import org.density.script.ScriptEngine;
import org.density.script.ScriptService;
import org.density.script.UpdateScript;
import org.density.search.aggregations.pipeline.MovingFunctionScript;
import org.density.threadpool.ThreadPool;
import org.density.transport.client.Client;
import org.density.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Registers Painless as a plugin.
 */
public final class PainlessModulePlugin extends Plugin implements ScriptPlugin, ExtensiblePlugin, ActionPlugin {

    private static final Map<ScriptContext<?>, List<Allowlist>> allowlists;

    /*
     * Contexts from Core that need custom allowlists can add them to the map below.
     * Allowlist resources should be added as appropriately named, separate files
     * under Painless' resources
     */
    static {
        Map<ScriptContext<?>, List<Allowlist>> map = new HashMap<>();

        // Moving Function Pipeline Agg
        List<Allowlist> movFn = new ArrayList<>(Allowlist.BASE_ALLOWLISTS);
        movFn.add(AllowlistLoader.loadFromResourceFiles(Allowlist.class, "org.density.aggs.movfn.txt"));
        map.put(MovingFunctionScript.CONTEXT, movFn);

        // Functions used for scoring docs
        List<Allowlist> scoreFn = new ArrayList<>(Allowlist.BASE_ALLOWLISTS);
        scoreFn.add(AllowlistLoader.loadFromResourceFiles(Allowlist.class, "org.density.score.txt"));
        map.put(ScoreScript.CONTEXT, scoreFn);

        // Functions available to ingest pipelines
        List<Allowlist> ingest = new ArrayList<>(Allowlist.BASE_ALLOWLISTS);
        ingest.add(AllowlistLoader.loadFromResourceFiles(Allowlist.class, "org.density.ingest.txt"));
        map.put(IngestScript.CONTEXT, ingest);

        // Functions available to update scripts
        List<Allowlist> update = new ArrayList<>(Allowlist.BASE_ALLOWLISTS);
        update.add(AllowlistLoader.loadFromResourceFiles(Allowlist.class, "org.density.update.txt"));
        map.put(UpdateScript.CONTEXT, update);

        // Functions available to derived fields
        List<Allowlist> derived = new ArrayList<>(Allowlist.BASE_ALLOWLISTS);
        derived.add(AllowlistLoader.loadFromResourceFiles(Allowlist.class, "org.density.derived.txt"));
        map.put(DerivedFieldScript.CONTEXT, derived);

        allowlists = map;
    }

    private final SetOnce<PainlessScriptEngine> painlessScriptEngine = new SetOnce<>();

    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        Map<ScriptContext<?>, List<Allowlist>> contextsWithAllowlists = new HashMap<>();
        for (ScriptContext<?> context : contexts) {
            // we might have a context that only uses the base allowlists, so would not have been filled in by reloadSPI
            List<Allowlist> contextAllowlists = allowlists.get(context);
            if (contextAllowlists == null) {
                contextAllowlists = new ArrayList<>(Allowlist.BASE_ALLOWLISTS);
            }
            contextsWithAllowlists.put(context, contextAllowlists);
        }
        painlessScriptEngine.set(new PainlessScriptEngine(settings, contextsWithAllowlists));
        return painlessScriptEngine.get();
    }

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
        IndexNameExpressionResolver expressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        // this is a hack to bind the painless script engine in guice (all components are added to guice), so that
        // the painless context api. this is a temporary measure until transport actions do no require guice
        return Collections.singletonList(painlessScriptEngine.get());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(CompilerSettings.REGEX_ENABLED, CompilerSettings.REGEX_LIMIT_FACTOR);
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        loader.loadExtensions(PainlessExtension.class)
            .stream()
            .flatMap(extension -> extension.getContextAllowlists().entrySet().stream())
            .forEach(entry -> {
                List<Allowlist> existing = allowlists.computeIfAbsent(entry.getKey(), c -> new ArrayList<>(Allowlist.BASE_ALLOWLISTS));
                existing.addAll(entry.getValue());
            });
    }

    @Override
    public List<ScriptContext<?>> getContexts() {
        return Collections.singletonList(PainlessExecuteAction.PainlessTestScript.CONTEXT);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>();
        actions.add(new ActionHandler<>(PainlessExecuteAction.INSTANCE, PainlessExecuteAction.TransportAction.class));
        actions.add(new ActionHandler<>(PainlessContextAction.INSTANCE, PainlessContextAction.TransportAction.class));
        return actions;
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
        List<RestHandler> handlers = new ArrayList<>();
        handlers.add(new PainlessExecuteAction.RestAction());
        handlers.add(new PainlessContextAction.RestAction());
        return handlers;
    }
}
