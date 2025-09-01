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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.density.transport.client;

import org.density.action.admin.indices.segments.IndicesSegmentResponse;
import org.density.action.admin.indices.segments.PitSegmentsRequest;
import org.density.action.admin.indices.view.ListViewNamesAction;
import org.density.action.admin.indices.view.SearchViewAction;
import org.density.action.bulk.BulkRequest;
import org.density.action.bulk.BulkRequestBuilder;
import org.density.action.bulk.BulkResponse;
import org.density.action.delete.DeleteRequest;
import org.density.action.delete.DeleteRequestBuilder;
import org.density.action.delete.DeleteResponse;
import org.density.action.explain.ExplainRequest;
import org.density.action.explain.ExplainRequestBuilder;
import org.density.action.explain.ExplainResponse;
import org.density.action.fieldcaps.FieldCapabilitiesRequest;
import org.density.action.fieldcaps.FieldCapabilitiesRequestBuilder;
import org.density.action.fieldcaps.FieldCapabilitiesResponse;
import org.density.action.get.GetRequest;
import org.density.action.get.GetRequestBuilder;
import org.density.action.get.GetResponse;
import org.density.action.get.MultiGetRequest;
import org.density.action.get.MultiGetRequestBuilder;
import org.density.action.get.MultiGetResponse;
import org.density.action.index.IndexRequest;
import org.density.action.index.IndexRequestBuilder;
import org.density.action.index.IndexResponse;
import org.density.action.search.ClearScrollRequest;
import org.density.action.search.ClearScrollRequestBuilder;
import org.density.action.search.ClearScrollResponse;
import org.density.action.search.CreatePitRequest;
import org.density.action.search.CreatePitResponse;
import org.density.action.search.DeletePitRequest;
import org.density.action.search.DeletePitResponse;
import org.density.action.search.GetAllPitNodesRequest;
import org.density.action.search.GetAllPitNodesResponse;
import org.density.action.search.MultiSearchRequest;
import org.density.action.search.MultiSearchRequestBuilder;
import org.density.action.search.MultiSearchResponse;
import org.density.action.search.SearchRequest;
import org.density.action.search.SearchRequestBuilder;
import org.density.action.search.SearchResponse;
import org.density.action.search.SearchScrollRequest;
import org.density.action.search.SearchScrollRequestBuilder;
import org.density.action.termvectors.MultiTermVectorsRequest;
import org.density.action.termvectors.MultiTermVectorsRequestBuilder;
import org.density.action.termvectors.MultiTermVectorsResponse;
import org.density.action.termvectors.TermVectorsRequest;
import org.density.action.termvectors.TermVectorsRequestBuilder;
import org.density.action.termvectors.TermVectorsResponse;
import org.density.action.update.UpdateRequest;
import org.density.action.update.UpdateRequestBuilder;
import org.density.action.update.UpdateResponse;
import org.density.common.Nullable;
import org.density.common.action.ActionFuture;
import org.density.common.annotation.PublicApi;
import org.density.common.lease.Releasable;
import org.density.common.settings.Setting;
import org.density.common.settings.Setting.Property;
import org.density.common.settings.Settings;
import org.density.core.action.ActionListener;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * A client provides a one stop interface for performing actions/operations against the cluster.
 * <p>
 * All operations performed are asynchronous by nature. Each action/operation has two flavors, the first
 * simply returns an {@link ActionFuture}, while the second accepts an
 * {@link ActionListener}.
 * <p>
 * A client can be retrieved from a started {@link org.density.node.Node}.
 *
 * @see org.density.node.Node#client()
 *
 * @density.api
 */
@PublicApi(since = "1.0.0")
public interface Client extends DensityClient, Releasable {

    Setting<String> CLIENT_TYPE_SETTING_S = new Setting<>("client.type", "node", (s) -> {
        switch (s) {
            case "node":
            case "transport":
                return s;
            default:
                throw new IllegalArgumentException("Can't parse [client.type] must be one of [node, transport]");
        }
    }, Property.NodeScope);

    /**
     * The admin client that can be used to perform administrative operations.
     */
    AdminClient admin();

    /**
     * Index a JSON source associated with a given index.
     * <p>
     * The id is optional, if it is not provided, one will be generated automatically.
     *
     * @param request The index request
     * @return The result future
     * @see Requests#indexRequest(String)
     */
    ActionFuture<IndexResponse> index(IndexRequest request);

    /**
     * Index a document associated with a given index.
     * <p>
     * The id is optional, if it is not provided, one will be generated automatically.
     *
     * @param request  The index request
     * @param listener A listener to be notified with a result
     * @see Requests#indexRequest(String)
     */
    void index(IndexRequest request, ActionListener<IndexResponse> listener);

    /**
     * Index a document associated with a given index.
     * <p>
     * The id is optional, if it is not provided, one will be generated automatically.
     */
    IndexRequestBuilder prepareIndex();

    /**
     * Index a document associated with a given index.
     * <p>
     * The id is optional, if it is not provided, one will be generated automatically.
     *
     * @param index The index to index the document to
     */
    IndexRequestBuilder prepareIndex(String index);

    /**
     * Updates a document based on a script.
     *
     * @param request The update request
     * @return The result future
     */
    ActionFuture<UpdateResponse> update(UpdateRequest request);

    /**
     * Updates a document based on a script.
     *
     * @param request  The update request
     * @param listener A listener to be notified with a result
     */
    void update(UpdateRequest request, ActionListener<UpdateResponse> listener);

    /**
     * Updates a document based on a script.
     */
    UpdateRequestBuilder prepareUpdate();

    /**
     * Updates a document based on a script.
     */
    UpdateRequestBuilder prepareUpdate(String index, String id);

    /**
     * Deletes a document from the index based on the index, and id.
     *
     * @param request The delete request
     * @return The result future
     * @see Requests#deleteRequest(String)
     */
    ActionFuture<DeleteResponse> delete(DeleteRequest request);

    /**
     * Deletes a document from the index based on the index, and id.
     *
     * @param request  The delete request
     * @param listener A listener to be notified with a result
     * @see Requests#deleteRequest(String)
     */
    void delete(DeleteRequest request, ActionListener<DeleteResponse> listener);

    /**
     * Deletes a document from the index based on the index, and id.
     */
    DeleteRequestBuilder prepareDelete();

    /**
     * Deletes a document from the index based on the index, and id.
     *
     * @param index The index to delete the document from
     * @param id    The id of the document to delete
     */
    DeleteRequestBuilder prepareDelete(String index, String id);

    /**
     * Executes a bulk of index / delete operations.
     *
     * @param request The bulk request
     * @return The result future
     * @see Requests#bulkRequest()
     */
    ActionFuture<BulkResponse> bulk(BulkRequest request);

    /**
     * Executes a bulk of index / delete operations.
     *
     * @param request  The bulk request
     * @param listener A listener to be notified with a result
     * @see Requests#bulkRequest()
     */
    void bulk(BulkRequest request, ActionListener<BulkResponse> listener);

    /**
     * Executes a bulk of index / delete operations.
     */
    BulkRequestBuilder prepareBulk();

    /**
     * Executes a bulk of index / delete operations with default index
     */
    BulkRequestBuilder prepareBulk(@Nullable String globalIndex);

    /**
     * Gets the document that was indexed from an index with an id.
     *
     * @param request The get request
     * @return The result future
     * @see Requests#getRequest(String)
     */
    ActionFuture<GetResponse> get(GetRequest request);

    /**
     * Gets the document that was indexed from an index with an id.
     *
     * @param request  The get request
     * @param listener A listener to be notified with a result
     * @see Requests#getRequest(String)
     */
    void get(GetRequest request, ActionListener<GetResponse> listener);

    /**
     * Gets the document that was indexed from an index with an id.
     */
    GetRequestBuilder prepareGet();

    /**
     * Gets the document that was indexed from an index with an id.
     */
    GetRequestBuilder prepareGet(String index, String id);

    /**
     * Multi get documents.
     */
    ActionFuture<MultiGetResponse> multiGet(MultiGetRequest request);

    /**
     * Multi get documents.
     */
    void multiGet(MultiGetRequest request, ActionListener<MultiGetResponse> listener);

    /**
     * Multi get documents.
     */
    MultiGetRequestBuilder prepareMultiGet();

    /**
     * Search across one or more indices with a query.
     *
     * @param request The search request
     * @return The result future
     * @see Requests#searchRequest(String...)
     */
    ActionFuture<SearchResponse> search(SearchRequest request);

    /**
     * Search across one or more indices with a query.
     *
     * @param request  The search request
     * @param listener A listener to be notified of the result
     * @see Requests#searchRequest(String...)
     */
    void search(SearchRequest request, ActionListener<SearchResponse> listener);

    /**
     * Search across one or more indices with a query.
     */
    SearchRequestBuilder prepareSearch(String... indices);

    /**
     * Search across one or more indices with a query.
     */
    SearchRequestBuilder prepareStreamSearch(String... indices);

    /**
     * A search scroll request to continue searching a previous scrollable search request.
     *
     * @param request The search scroll request
     * @return The result future
     * @see Requests#searchScrollRequest(String)
     */
    ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request);

    /**
     * A search scroll request to continue searching a previous scrollable search request.
     *
     * @param request  The search scroll request
     * @param listener A listener to be notified of the result
     * @see Requests#searchScrollRequest(String)
     */
    void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener);

    /**
     * A search scroll request to continue searching a previous scrollable search request.
     */
    SearchScrollRequestBuilder prepareSearchScroll(String scrollId);

    /**
     * Create point in time for one or more indices
     */
    void createPit(CreatePitRequest createPITRequest, ActionListener<CreatePitResponse> listener);

    /**
     * Delete one or more point in time contexts
     */
    void deletePits(DeletePitRequest deletePITRequest, ActionListener<DeletePitResponse> listener);

    /**
     * Get all active point in time searches
     */
    void getAllPits(GetAllPitNodesRequest getAllPitNodesRequest, ActionListener<GetAllPitNodesResponse> listener);

    /**
     * Get information of segments of one or more PITs
     */
    void pitSegments(PitSegmentsRequest pitSegmentsRequest, ActionListener<IndicesSegmentResponse> listener);

    /**
     * Performs multiple search requests.
     */
    ActionFuture<MultiSearchResponse> multiSearch(MultiSearchRequest request);

    /**
     * Performs multiple search requests.
     */
    void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener);

    /**
     * Performs multiple search requests.
     */
    MultiSearchRequestBuilder prepareMultiSearch();

    /**
     * An action that returns the term vectors for a specific document.
     *
     * @param request The term vector request
     * @return The response future
     */
    ActionFuture<TermVectorsResponse> termVectors(TermVectorsRequest request);

    /**
     * An action that returns the term vectors for a specific document.
     *
     * @param request The term vector request
     */
    void termVectors(TermVectorsRequest request, ActionListener<TermVectorsResponse> listener);

    /**
     * Builder for the term vector request.
     */
    TermVectorsRequestBuilder prepareTermVectors();

    /**
     * Builder for the term vector request.
     *
     * @param index The index to load the document from
     * @param id    The id of the document
     */
    TermVectorsRequestBuilder prepareTermVectors(String index, String id);

    /**
     * Multi get term vectors.
     */
    ActionFuture<MultiTermVectorsResponse> multiTermVectors(MultiTermVectorsRequest request);

    /**
     * Multi get term vectors.
     */
    void multiTermVectors(MultiTermVectorsRequest request, ActionListener<MultiTermVectorsResponse> listener);

    /**
     * Multi get term vectors.
     */
    MultiTermVectorsRequestBuilder prepareMultiTermVectors();

    /**
     * Computes a score explanation for the specified request.
     *
     * @param index The index this explain is targeted for
     * @param id    The document identifier this explain is targeted for
     */
    ExplainRequestBuilder prepareExplain(String index, String id);

    /**
     * Computes a score explanation for the specified request.
     *
     * @param request The request encapsulating the query and document identifier to compute a score explanation for
     */
    ActionFuture<ExplainResponse> explain(ExplainRequest request);

    /**
     * Computes a score explanation for the specified request.
     *
     * @param request  The request encapsulating the query and document identifier to compute a score explanation for
     * @param listener A listener to be notified of the result
     */
    void explain(ExplainRequest request, ActionListener<ExplainResponse> listener);

    /**
     * Clears the search contexts associated with specified scroll ids.
     */
    ClearScrollRequestBuilder prepareClearScroll();

    /**
     * Clears the search contexts associated with specified scroll ids.
     */
    ActionFuture<ClearScrollResponse> clearScroll(ClearScrollRequest request);

    /**
     * Clears the search contexts associated with specified scroll ids.
     */
    void clearScroll(ClearScrollRequest request, ActionListener<ClearScrollResponse> listener);

    /**
     * Builder for the field capabilities request.
     */
    FieldCapabilitiesRequestBuilder prepareFieldCaps(String... indices);

    /**
     * An action that returns the field capabilities from the provided request
     */
    ActionFuture<FieldCapabilitiesResponse> fieldCaps(FieldCapabilitiesRequest request);

    /**
     * An action that returns the field capabilities from the provided request
     */
    void fieldCaps(FieldCapabilitiesRequest request, ActionListener<FieldCapabilitiesResponse> listener);

    /** Search a view */
    void searchView(final SearchViewAction.Request request, final ActionListener<SearchResponse> listener);

    /** Search a view */
    ActionFuture<SearchResponse> searchView(final SearchViewAction.Request request);

    /** List all view names */
    void listViewNames(final ListViewNamesAction.Request request, ActionListener<ListViewNamesAction.Response> listener);

    /** List all view names */
    ActionFuture<ListViewNamesAction.Response> listViewNames(final ListViewNamesAction.Request request);

    /**
     * Returns this clients settings
     */
    Settings settings();

    /**
     * Returns a new lightweight Client that applies all given headers to each of the requests
     * issued from it.
     */
    Client filterWithHeader(Map<String, String> headers);

    /**
     * Returns a client to a remote cluster with the given cluster alias.
     *
     * @throws IllegalArgumentException if the given clusterAlias doesn't exist
     * @throws UnsupportedOperationException if this functionality is not available on this client.
     */
    default Client getRemoteClusterClient(String clusterAlias) {
        throw new UnsupportedOperationException("this client doesn't support remote cluster connections");
    }

    /**
     * Index a document - CompletionStage version
     */
    default CompletionStage<IndexResponse> indexAsync(IndexRequest request) {
        CompletableFuture<IndexResponse> future = new CompletableFuture<>();
        index(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Update a document - CompletionStage version
     */
    default CompletionStage<UpdateResponse> updateAsync(UpdateRequest request) {
        CompletableFuture<UpdateResponse> future = new CompletableFuture<>();
        update(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Delete a document - CompletionStage version
     */
    default CompletionStage<DeleteResponse> deleteAsync(DeleteRequest request) {
        CompletableFuture<DeleteResponse> future = new CompletableFuture<>();
        delete(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Bulk operations - CompletionStage version
     */
    default CompletionStage<BulkResponse> bulkAsync(BulkRequest request) {
        CompletableFuture<BulkResponse> future = new CompletableFuture<>();
        bulk(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Get document - CompletionStage version
     */
    default CompletionStage<GetResponse> getAsync(GetRequest request) {
        CompletableFuture<GetResponse> future = new CompletableFuture<>();
        get(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Multi get - CompletionStage version
     */
    default CompletionStage<MultiGetResponse> multiGetAsync(MultiGetRequest request) {
        CompletableFuture<MultiGetResponse> future = new CompletableFuture<>();
        multiGet(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Search - CompletionStage version
     */
    default CompletionStage<SearchResponse> searchAsync(SearchRequest request) {
        CompletableFuture<SearchResponse> future = new CompletableFuture<>();
        search(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Search scroll - CompletionStage version
     */
    default CompletionStage<SearchResponse> searchScrollAsync(SearchScrollRequest request) {
        CompletableFuture<SearchResponse> future = new CompletableFuture<>();
        searchScroll(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Multi search - CompletionStage version
     */
    default CompletionStage<MultiSearchResponse> multiSearchAsync(MultiSearchRequest request) {
        CompletableFuture<MultiSearchResponse> future = new CompletableFuture<>();
        multiSearch(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Term vectors - CompletionStage version
     */
    default CompletionStage<TermVectorsResponse> termVectorsAsync(TermVectorsRequest request) {
        CompletableFuture<TermVectorsResponse> future = new CompletableFuture<>();
        termVectors(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Multi term vectors - CompletionStage version
     */
    default CompletionStage<MultiTermVectorsResponse> multiTermVectorsAsync(MultiTermVectorsRequest request) {
        CompletableFuture<MultiTermVectorsResponse> future = new CompletableFuture<>();
        multiTermVectors(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Explain - CompletionStage version
     */
    default CompletionStage<ExplainResponse> explainAsync(ExplainRequest request) {
        CompletableFuture<ExplainResponse> future = new CompletableFuture<>();
        explain(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Clear scroll - CompletionStage version
     */
    default CompletionStage<ClearScrollResponse> clearScrollAsync(ClearScrollRequest request) {
        CompletableFuture<ClearScrollResponse> future = new CompletableFuture<>();
        clearScroll(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Field capabilities - CompletionStage version
     */
    default CompletionStage<FieldCapabilitiesResponse> fieldCapsAsync(FieldCapabilitiesRequest request) {
        CompletableFuture<FieldCapabilitiesResponse> future = new CompletableFuture<>();
        fieldCaps(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * Search view - CompletionStage version
     */
    default CompletionStage<SearchResponse> searchViewAsync(SearchViewAction.Request request) {
        CompletableFuture<SearchResponse> future = new CompletableFuture<>();
        searchView(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }

    /**
     * List view names - CompletionStage version
     */
    default CompletionStage<ListViewNamesAction.Response> listViewNamesAsync(ListViewNamesAction.Request request) {
        CompletableFuture<ListViewNamesAction.Response> future = new CompletableFuture<>();
        listViewNames(request, ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }
}
