/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.rule;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.ExceptionsHelper;
import org.density.ResourceAlreadyExistsException;
import org.density.ResourceNotFoundException;
import org.density.action.ActionListenerResponseHandler;
import org.density.action.ActionRequest;
import org.density.action.admin.indices.create.CreateIndexRequest;
import org.density.action.admin.indices.create.CreateIndexResponse;
import org.density.cluster.ClusterState;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.routing.IndexShardRoutingTable;
import org.density.cluster.routing.ShardRouting;
import org.density.cluster.service.ClusterService;
import org.density.common.util.concurrent.ThreadContext;
import org.density.core.action.ActionListener;
import org.density.core.action.ActionResponse;
import org.density.core.common.io.stream.Writeable;
import org.density.plugin.wlm.WorkloadManagementPlugin;
import org.density.rule.RuleRoutingService;
import org.density.rule.action.CreateRuleAction;
import org.density.rule.action.CreateRuleRequest;
import org.density.rule.action.CreateRuleResponse;
import org.density.rule.action.UpdateRuleAction;
import org.density.rule.action.UpdateRuleRequest;
import org.density.rule.action.UpdateRuleResponse;
import org.density.transport.TransportService;
import org.density.transport.client.Client;

import java.util.Map;
import java.util.Optional;

/**
 * Service responsible for routing CreateRule requests to the correct node based on primary shard ownership.
 * @density.experimental
 */
public class WorkloadGroupRuleRoutingService implements RuleRoutingService {
    private final Client client;
    private final ClusterService clusterService;
    private TransportService transportService;
    private static final Logger logger = LogManager.getLogger(WorkloadGroupRuleRoutingService.class);
    private static final Map<String, Object> indexSettings = Map.of("index.number_of_shards", 1, "index.auto_expand_replicas", "0-all");

    /**
     * Constructor for WorkloadGroupRuleRoutingService
     * @param client
     * @param clusterService
     */
    public WorkloadGroupRuleRoutingService(Client client, ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
    }

    /**
     * Set {@link TransportService} for WorkloadGroupRuleRoutingService
     * @param transportService
     */
    public void setTransportService(TransportService transportService) {
        this.transportService = transportService;
    }

    @Override
    public void handleCreateRuleRequest(CreateRuleRequest request, ActionListener<CreateRuleResponse> listener) {
        String indexName = WorkloadManagementPlugin.INDEX_NAME;

        try (ThreadContext.StoredContext ctx = client.threadPool().getThreadContext().stashContext()) {
            if (hasIndex(indexName)) {
                routeRequest(CreateRuleAction.NAME, indexName, request, CreateRuleResponse::new, listener);
                return;
            }
            createIndex(indexName, new ActionListener<>() {
                @Override
                public void onResponse(CreateIndexResponse response) {
                    if (!response.isAcknowledged()) {
                        logger.error("Failed to create index " + indexName);
                        listener.onFailure(new IllegalStateException(indexName + " index creation not acknowledged"));
                    } else {
                        routeRequest(CreateRuleAction.NAME, indexName, request, CreateRuleResponse::new, listener);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    Throwable cause = ExceptionsHelper.unwrapCause(e);
                    if (cause instanceof ResourceAlreadyExistsException) {
                        routeRequest(CreateRuleAction.NAME, indexName, request, CreateRuleResponse::new, listener);
                    } else {
                        logger.error("Failed to create index {}: {}", indexName, e.getMessage());
                        listener.onFailure(e);
                    }
                }
            });
        }
    }

    @Override
    public void handleUpdateRuleRequest(UpdateRuleRequest request, ActionListener<UpdateRuleResponse> listener) {
        String indexName = WorkloadManagementPlugin.INDEX_NAME;
        try (ThreadContext.StoredContext ctx = client.threadPool().getThreadContext().stashContext()) {
            if (!hasIndex(indexName)) {
                logger.error("Index {} not found", indexName);
                listener.onFailure(new ResourceNotFoundException("Index " + indexName + " does not exist."));
            } else {
                routeRequest(UpdateRuleAction.NAME, indexName, request, UpdateRuleResponse::new, listener);
            }
        }
    }

    /**
     * Creates the backing index if it does not exist, then runs the given success callback.
     * @param indexName the name of the index to create
     * @param listener listener to handle failures
     */
    private void createIndex(String indexName, ActionListener<CreateIndexResponse> listener) {
        final CreateIndexRequest createRequest = new CreateIndexRequest(indexName).settings(indexSettings);
        client.admin().indices().create(createRequest, listener);
    }

    /**
     * Routes the request to the primary shard node for the given index.
     * Executes locally if the current node is the primary.
     * @param actionName
     * @param indexName
     * @param request
     * @param responseReader
     * @param listener
     */
    private <Request extends ActionRequest, Response extends ActionResponse> void routeRequest(
        String actionName,
        String indexName,
        Request request,
        Writeable.Reader<Response> responseReader,
        ActionListener<Response> listener
    ) {
        Optional<DiscoveryNode> primaryNodeOpt = getPrimaryShardNode(indexName);
        if (primaryNodeOpt.isEmpty()) {
            listener.onFailure(new IllegalStateException("Primary node for index [" + indexName + "] not found"));
            return;
        }
        transportService.sendRequest(
            primaryNodeOpt.get(),
            actionName,
            request,
            new ActionListenerResponseHandler<>(listener, responseReader)
        );
    }

    /**
     * Retrieves the discovery node that holds the primary shard for the given index.
     * @param indexName the index name
     */
    private Optional<DiscoveryNode> getPrimaryShardNode(String indexName) {
        ClusterState state = clusterService.state();
        return Optional.ofNullable(state.getRoutingTable().index(indexName))
            .map(table -> table.shard(0))
            .map(IndexShardRoutingTable::primaryShard)
            .filter(ShardRouting::assignedToNode)
            .map(shard -> state.nodes().get(shard.currentNodeId()));
    }

    /**
     * Checks whether the index is present
     * @param indexName - the index name to check
     */
    private boolean hasIndex(String indexName) {
        return clusterService.state().metadata().hasIndex(indexName);
    }
}
