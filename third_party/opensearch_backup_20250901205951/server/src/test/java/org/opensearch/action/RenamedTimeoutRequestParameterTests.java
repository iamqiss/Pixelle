/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action;

import org.density.DensityParseException;
import org.density.action.support.clustermanager.ClusterManagerNodeRequest;
import org.density.common.breaker.ResponseLimitSettings;
import org.density.common.logging.DeprecationLogger;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.common.settings.SettingsFilter;
import org.density.core.common.bytes.BytesArray;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.rest.BaseRestHandler;
import org.density.rest.action.admin.cluster.RestCleanupRepositoryAction;
import org.density.rest.action.admin.cluster.RestCloneSnapshotAction;
import org.density.rest.action.admin.cluster.RestClusterGetSettingsAction;
import org.density.rest.action.admin.cluster.RestClusterHealthAction;
import org.density.rest.action.admin.cluster.RestClusterRerouteAction;
import org.density.rest.action.admin.cluster.RestClusterStateAction;
import org.density.rest.action.admin.cluster.RestClusterUpdateSettingsAction;
import org.density.rest.action.admin.cluster.RestCreateSnapshotAction;
import org.density.rest.action.admin.cluster.RestDeleteRepositoryAction;
import org.density.rest.action.admin.cluster.RestDeleteSnapshotAction;
import org.density.rest.action.admin.cluster.RestDeleteStoredScriptAction;
import org.density.rest.action.admin.cluster.RestGetRepositoriesAction;
import org.density.rest.action.admin.cluster.RestGetSnapshotsAction;
import org.density.rest.action.admin.cluster.RestGetStoredScriptAction;
import org.density.rest.action.admin.cluster.RestPutRepositoryAction;
import org.density.rest.action.admin.cluster.RestPutStoredScriptAction;
import org.density.rest.action.admin.cluster.RestRestoreSnapshotAction;
import org.density.rest.action.admin.cluster.RestSnapshotsStatusAction;
import org.density.rest.action.admin.cluster.RestVerifyRepositoryAction;
import org.density.rest.action.admin.cluster.dangling.RestDeleteDanglingIndexAction;
import org.density.rest.action.admin.cluster.dangling.RestImportDanglingIndexAction;
import org.density.rest.action.admin.indices.RestAddIndexBlockAction;
import org.density.rest.action.admin.indices.RestCloseIndexAction;
import org.density.rest.action.admin.indices.RestCreateIndexAction;
import org.density.rest.action.admin.indices.RestDeleteComponentTemplateAction;
import org.density.rest.action.admin.indices.RestDeleteComposableIndexTemplateAction;
import org.density.rest.action.admin.indices.RestDeleteIndexAction;
import org.density.rest.action.admin.indices.RestDeleteIndexTemplateAction;
import org.density.rest.action.admin.indices.RestGetComponentTemplateAction;
import org.density.rest.action.admin.indices.RestGetComposableIndexTemplateAction;
import org.density.rest.action.admin.indices.RestGetIndexTemplateAction;
import org.density.rest.action.admin.indices.RestGetIndicesAction;
import org.density.rest.action.admin.indices.RestGetMappingAction;
import org.density.rest.action.admin.indices.RestGetSettingsAction;
import org.density.rest.action.admin.indices.RestIndexDeleteAliasesAction;
import org.density.rest.action.admin.indices.RestIndexPutAliasAction;
import org.density.rest.action.admin.indices.RestIndicesAliasesAction;
import org.density.rest.action.admin.indices.RestOpenIndexAction;
import org.density.rest.action.admin.indices.RestPutComponentTemplateAction;
import org.density.rest.action.admin.indices.RestPutComposableIndexTemplateAction;
import org.density.rest.action.admin.indices.RestPutIndexTemplateAction;
import org.density.rest.action.admin.indices.RestPutMappingAction;
import org.density.rest.action.admin.indices.RestResizeHandler;
import org.density.rest.action.admin.indices.RestRolloverIndexAction;
import org.density.rest.action.admin.indices.RestSimulateIndexTemplateAction;
import org.density.rest.action.admin.indices.RestSimulateTemplateAction;
import org.density.rest.action.admin.indices.RestUpdateSettingsAction;
import org.density.rest.action.cat.RestAllocationAction;
import org.density.rest.action.cat.RestClusterManagerAction;
import org.density.rest.action.cat.RestIndicesAction;
import org.density.rest.action.cat.RestNodeAttrsAction;
import org.density.rest.action.cat.RestNodesAction;
import org.density.rest.action.cat.RestPendingClusterTasksAction;
import org.density.rest.action.cat.RestPluginsAction;
import org.density.rest.action.cat.RestRepositoriesAction;
import org.density.rest.action.cat.RestSegmentsAction;
import org.density.rest.action.cat.RestShardsAction;
import org.density.rest.action.cat.RestSnapshotAction;
import org.density.rest.action.cat.RestTemplatesAction;
import org.density.rest.action.cat.RestThreadPoolAction;
import org.density.rest.action.ingest.RestDeletePipelineAction;
import org.density.rest.action.ingest.RestGetPipelineAction;
import org.density.rest.action.ingest.RestPutPipelineAction;
import org.density.test.DensityTestCase;
import org.density.test.rest.FakeRestRequest;
import org.density.threadpool.TestThreadPool;
import org.density.transport.client.node.NodeClient;
import org.junit.After;

import java.io.IOException;
import java.util.Collections;

import static org.density.cluster.metadata.IndexMetadata.INDEX_READ_ONLY_SETTING;
import static org.hamcrest.Matchers.containsString;

/**
 * As of 2.0, the request parameter 'master_timeout' in all applicable REST APIs is deprecated,
 * and alternative parameter 'cluster_manager_timeout' is added.
 * The tests are used to validate the behavior about the renamed request parameter.
 * Remove the test after removing MASTER_ROLE and 'master_timeout'.
 */
public class RenamedTimeoutRequestParameterTests extends DensityTestCase {
    private final TestThreadPool threadPool = new TestThreadPool(RenamedTimeoutRequestParameterTests.class.getName());
    private final NodeClient client = new NodeClient(Settings.EMPTY, threadPool);
    private final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RenamedTimeoutRequestParameterTests.class);

    private static final String DUPLICATE_PARAMETER_ERROR_MESSAGE =
        "Please only use one of the request parameters [master_timeout, cluster_manager_timeout].";
    private static final String MASTER_TIMEOUT_DEPRECATED_MESSAGE =
        "Parameter [master_timeout] is deprecated and will be removed in 3.0. To support inclusive language, please use [cluster_manager_timeout] instead.";

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    public void testNoWarningsForNewParam() {
        BaseRestHandler.parseDeprecatedMasterTimeoutParameter(
            getMasterNodeRequest(),
            getRestRequestWithNewParam(),
            deprecationLogger,
            "test"
        );
    }

    public void testDeprecationWarningForOldParam() {
        BaseRestHandler.parseDeprecatedMasterTimeoutParameter(
            getMasterNodeRequest(),
            getRestRequestWithDeprecatedParam(),
            deprecationLogger,
            "test"
        );
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testBothParamsNotValid() {
        Exception e = assertThrows(
            DensityParseException.class,
            () -> BaseRestHandler.parseDeprecatedMasterTimeoutParameter(
                getMasterNodeRequest(),
                getRestRequestWithBothParams(),
                deprecationLogger,
                "test"
            )
        );
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatAllocation() {
        RestAllocationAction action = new RestAllocationAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatIndices() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        Settings settings = Settings.builder().build();
        ResponseLimitSettings responseLimitSettings = new ResponseLimitSettings(clusterSettings, settings);
        RestIndicesAction action = new RestIndicesAction(responseLimitSettings);
        Exception e = assertThrows(DensityParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatClusterManager() {
        RestClusterManagerAction action = new RestClusterManagerAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatMaster() {
        RestClusterManagerAction action = new RestClusterManagerAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatNodeattrs() {
        RestNodeAttrsAction action = new RestNodeAttrsAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatNodes() {
        RestNodesAction action = new RestNodesAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatPendingTasks() {
        RestPendingClusterTasksAction action = new RestPendingClusterTasksAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatPlugins() {
        RestPluginsAction action = new RestPluginsAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatRepositories() {
        RestRepositoriesAction action = new RestRepositoriesAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatShards() {
        RestShardsAction action = new RestShardsAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatSnapshots() {
        RestSnapshotAction action = new RestSnapshotAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatTemplates() {
        RestTemplatesAction action = new RestTemplatesAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatThreadPool() {
        RestThreadPoolAction action = new RestThreadPoolAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatSegments() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final Settings settings = Settings.builder().build();
        final ResponseLimitSettings responseLimitSettings = new ResponseLimitSettings(clusterSettings, settings);
        RestSegmentsAction action = new RestSegmentsAction(responseLimitSettings);
        Exception e = assertThrows(DensityParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testClusterHealth() {
        Exception e = assertThrows(
            DensityParseException.class,
            () -> RestClusterHealthAction.fromRequest(getRestRequestWithBodyWithBothParams())
        );
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testClusterReroute() throws IOException {
        final SettingsFilter filter = new SettingsFilter(Collections.singleton("foo.filtered"));
        RestClusterRerouteAction action = new RestClusterRerouteAction(filter);
        Exception e = assertThrows(
            DensityParseException.class,
            () -> action.prepareRequest(getRestRequestWithBodyWithBothParams(), client)
        );
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testClusterState() throws IOException {
        final SettingsFilter filter = new SettingsFilter(Collections.singleton("foo.filtered"));
        RestClusterStateAction action = new RestClusterStateAction(filter);
        Exception e = assertThrows(
            DensityParseException.class,
            () -> action.prepareRequest(getRestRequestWithBodyWithBothParams(), client)
        );
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testClusterGetSettings() throws IOException {
        final SettingsFilter filter = new SettingsFilter(Collections.singleton("foo.filtered"));
        RestClusterGetSettingsAction action = new RestClusterGetSettingsAction(null, null, filter);
        Exception e = assertThrows(
            DensityParseException.class,
            () -> action.prepareRequest(getRestRequestWithBodyWithBothParams(), client)
        );
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testClusterUpdateSettings() throws IOException {
        RestClusterUpdateSettingsAction action = new RestClusterUpdateSettingsAction();
        Exception e = assertThrows(
            DensityParseException.class,
            () -> action.prepareRequest(getRestRequestWithBodyWithBothParams(), client)
        );
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testClusterPendingTasks() {
        RestPendingClusterTasksAction action = new RestPendingClusterTasksAction();
        Exception e = assertThrows(
            DensityParseException.class,
            () -> action.prepareRequest(getRestRequestWithBodyWithBothParams(), client)
        );
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testAddIndexBlock() {
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("cluster_manager_timeout", "1h");
        request.params().put("master_timeout", "3s");
        request.params().put("block", "metadata");
        NodeClient client = new NodeClient(Settings.builder().put(INDEX_READ_ONLY_SETTING.getKey(), Boolean.FALSE).build(), threadPool);
        RestAddIndexBlockAction action = new RestAddIndexBlockAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(request, client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCloseIndex() {
        RestCloseIndexAction action = new RestCloseIndexAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCreateIndex() {
        RestCreateIndexAction action = new RestCreateIndexAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testDeleteIndex() {
        RestDeleteIndexAction action = new RestDeleteIndexAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testGetIndices() {
        RestGetIndicesAction action = new RestGetIndicesAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testGetMapping() {
        RestGetMappingAction action = new RestGetMappingAction(threadPool);
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testGetSettings() {
        RestGetSettingsAction action = new RestGetSettingsAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testIndexDeleteAliases() {
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("cluster_manager_timeout", "1h");
        request.params().put("master_timeout", "3s");
        request.params().put("name", "*");
        request.params().put("index", "test");
        RestIndexDeleteAliasesAction action = new RestIndexDeleteAliasesAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(request, client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testIndexPutAlias() {
        RestIndexPutAliasAction action = new RestIndexPutAliasAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testIndicesAliases() {
        RestIndicesAliasesAction action = new RestIndicesAliasesAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testOpenIndex() {
        RestOpenIndexAction action = new RestOpenIndexAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testPutMapping() {
        RestPutMappingAction action = new RestPutMappingAction();
        Exception e = assertThrows(
            DensityParseException.class,
            () -> action.prepareRequest(getRestRequestWithBodyWithBothParams(), client)
        );
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testShrinkIndex() {
        RestResizeHandler.RestShrinkIndexAction action = new RestResizeHandler.RestShrinkIndexAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testSplitIndex() {
        RestResizeHandler.RestSplitIndexAction action = new RestResizeHandler.RestSplitIndexAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCloneIndex() {
        RestResizeHandler.RestCloneIndexAction action = new RestResizeHandler.RestCloneIndexAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testRolloverIndex() {
        RestRolloverIndexAction action = new RestRolloverIndexAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testUpdateSettings() {
        RestUpdateSettingsAction action = new RestUpdateSettingsAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testDeleteDanglingIndex() {
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("cluster_manager_timeout", "1h");
        request.params().put("master_timeout", "3s");
        request.params().put("index_uuid", "test");
        RestDeleteDanglingIndexAction action = new RestDeleteDanglingIndexAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(request, client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testImportDanglingIndex() {
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("cluster_manager_timeout", "1h");
        request.params().put("master_timeout", "3s");
        request.params().put("index_uuid", "test");
        RestImportDanglingIndexAction action = new RestImportDanglingIndexAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(request, client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testDeleteComponentTemplate() {
        RestDeleteComponentTemplateAction action = new RestDeleteComponentTemplateAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testDeleteComposableIndexTemplate() {
        RestDeleteComposableIndexTemplateAction action = new RestDeleteComposableIndexTemplateAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testDeleteIndexTemplate() {
        RestDeleteIndexTemplateAction action = new RestDeleteIndexTemplateAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testGetComponentTemplate() {
        RestGetComponentTemplateAction action = new RestGetComponentTemplateAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testGetComposableIndexTemplate() {
        RestGetComposableIndexTemplateAction action = new RestGetComposableIndexTemplateAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testGetIndexTemplate() {
        RestGetIndexTemplateAction action = new RestGetIndexTemplateAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testPutComponentTemplate() {
        RestPutComponentTemplateAction action = new RestPutComponentTemplateAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testPutComposableIndexTemplate() {
        RestPutComposableIndexTemplateAction action = new RestPutComposableIndexTemplateAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testPutIndexTemplate() {
        RestPutIndexTemplateAction action = new RestPutIndexTemplateAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testSimulateIndexTemplate() {
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("cluster_manager_timeout", randomFrom("1h", "2m"));
        request.params().put("master_timeout", "3s");
        request.params().put("name", "test");
        RestSimulateIndexTemplateAction action = new RestSimulateIndexTemplateAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(request, client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testSimulateTemplate() {
        RestSimulateTemplateAction action = new RestSimulateTemplateAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCleanupRepository() {
        RestCleanupRepositoryAction action = new RestCleanupRepositoryAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCloneSnapshot() {
        RestCloneSnapshotAction action = new RestCloneSnapshotAction();
        Exception e = assertThrows(
            DensityParseException.class,
            () -> action.prepareRequest(getRestRequestWithBodyWithBothParams(), client)
        );
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCreateSnapshot() {
        RestCreateSnapshotAction action = new RestCreateSnapshotAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testDeleteRepository() {
        RestDeleteRepositoryAction action = new RestDeleteRepositoryAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testDeleteSnapshot() {
        RestDeleteSnapshotAction action = new RestDeleteSnapshotAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testGetRepositories() {
        final SettingsFilter filter = new SettingsFilter(Collections.singleton("foo.filtered"));
        RestGetRepositoriesAction action = new RestGetRepositoriesAction(filter);
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testGetSnapshots() {
        RestGetSnapshotsAction action = new RestGetSnapshotsAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testPutRepository() {
        RestPutRepositoryAction action = new RestPutRepositoryAction();
        Exception e = assertThrows(
            DensityParseException.class,
            () -> action.prepareRequest(getRestRequestWithBodyWithBothParams(), client)
        );
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testRestoreSnapshot() {
        RestRestoreSnapshotAction action = new RestRestoreSnapshotAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testSnapshotsStatus() {
        RestSnapshotsStatusAction action = new RestSnapshotsStatusAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testVerifyRepository() {
        RestVerifyRepositoryAction action = new RestVerifyRepositoryAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testDeletePipeline() {
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("cluster_manager_timeout", "1h");
        request.params().put("master_timeout", "3s");
        request.params().put("id", "test");
        RestDeletePipelineAction action = new RestDeletePipelineAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(request, client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testGetPipeline() {
        RestGetPipelineAction action = new RestGetPipelineAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testPutPipeline() {
        FakeRestRequest request = getFakeRestRequestWithBody();
        request.params().put("cluster_manager_timeout", "2m");
        request.params().put("master_timeout", "3s");
        request.params().put("id", "test");
        RestPutPipelineAction action = new RestPutPipelineAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(request, client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testDeleteStoredScript() {
        RestDeleteStoredScriptAction action = new RestDeleteStoredScriptAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testGetStoredScript() {
        RestGetStoredScriptAction action = new RestGetStoredScriptAction();
        Exception e = assertThrows(DensityParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testPutStoredScript() {
        RestPutStoredScriptAction action = new RestPutStoredScriptAction();
        Exception e = assertThrows(
            DensityParseException.class,
            () -> action.prepareRequest(getRestRequestWithBodyWithBothParams(), client)
        );
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE, "empty templates should no longer be used");
    }

    private ClusterManagerNodeRequest getMasterNodeRequest() {
        return new ClusterManagerNodeRequest() {
            @Override
            public ActionRequestValidationException validate() {
                return null;
            }
        };
    }

    private FakeRestRequest getRestRequestWithBothParams() {
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("cluster_manager_timeout", "1h");
        request.params().put("master_timeout", "3s");
        return request;
    }

    private FakeRestRequest getRestRequestWithDeprecatedParam() {
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("master_timeout", "3s");
        return request;
    }

    private FakeRestRequest getRestRequestWithNewParam() {
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("cluster_manager_timeout", "2m");
        return request;
    }

    private FakeRestRequest getRestRequestWithBodyWithBothParams() {
        FakeRestRequest request = getFakeRestRequestWithBody();
        request.params().put("cluster_manager_timeout", "2m");
        request.params().put("master_timeout", "3s");
        return request;
    }

    private FakeRestRequest getFakeRestRequestWithBody() {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(new BytesArray("{}"), MediaTypeRegistry.JSON).build();
    }
}
