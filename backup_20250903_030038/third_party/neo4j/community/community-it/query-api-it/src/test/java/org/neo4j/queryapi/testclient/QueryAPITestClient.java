/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.queryapi.testclient;

import static org.neo4j.queryapi.QueryApiTestUtil.encodedCredentials;
import static org.neo4j.test.assertion.Assert.assertEventually;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class QueryAPITestClient {

    private final HttpClient client;
    private final String endpoint;
    private final String credentials;
    private final ObjectMapper MAPPER = new ObjectMapper();
    private final boolean requiresTypedFormat;

    public QueryAPITestClient(String endpoint) {
        this(endpoint, null, null, false);
    }

    public QueryAPITestClient(String endpoint, String username, String password) {
        this(endpoint, username, password, false);
    }

    public QueryAPITestClient(String endpoint, boolean requiresTypedFormat) {
        this(endpoint, null, null, requiresTypedFormat);
    }

    public QueryAPITestClient(String endpoint, String username, String password, boolean requiresTypedFormat) {
        this.endpoint = endpoint;
        if (username != null && password != null) {
            this.credentials = encodedCredentials(username, password);
        } else {
            this.credentials = null;
        }
        this.client = HttpClient.newHttpClient();
        this.requiresTypedFormat = requiresTypedFormat;
        ensureServerIsAvailable();
    }

    public HttpResponse<QueryResponse> autoCommit(QueryRequest request) throws IOException, InterruptedException {
        return autoCommit(request, "neo4j");
    }

    public HttpResponse<QueryResponse> autoCommit(QueryRequest request, String database)
            throws IOException, InterruptedException {
        return sendRequest(request, endpoint.replace("{databaseName}", database));
    }

    public HttpResponse<QueryResponse> beginTx(QueryRequest request, String database)
            throws IOException, InterruptedException {
        return sendRequest(request, endpoint.replace("{databaseName}", database) + "/tx");
    }

    public HttpResponse<QueryResponse> beginTx(QueryRequest request) throws IOException, InterruptedException {
        return beginTx(request, "neo4j");
    }

    public HttpResponse<QueryResponse> beginTx() throws IOException, InterruptedException {
        return beginTx(null);
    }

    public HttpResponse<QueryResponse> runInTx(QueryRequest request, String txId, String database)
            throws IOException, InterruptedException {
        return sendRequest(request, endpoint.replace("{databaseName}", database) + "/tx/" + txId);
    }

    public HttpResponse<QueryResponse> runInTx(QueryRequest request, String txId)
            throws IOException, InterruptedException {
        return runInTx(request, txId, "neo4j");
    }

    public HttpResponse<QueryResponse> runInTx(String txId) throws IOException, InterruptedException {
        return runInTx(null, txId);
    }

    public HttpResponse<QueryResponse> commitTx(QueryRequest request, String txId, String database)
            throws IOException, InterruptedException {
        return sendRequest(request, endpoint.replace("{databaseName}", database) + "/tx/" + txId + "/commit");
    }

    public HttpResponse<QueryResponse> commitTx(QueryRequest request, String txId)
            throws IOException, InterruptedException {
        return commitTx(request, txId, "neo4j");
    }

    public HttpResponse<QueryResponse> commitTx(String txId) throws IOException, InterruptedException {
        return commitTx(null, txId, "neo4j");
    }

    public HttpResponse<QueryResponse> rollbackTx(String txId, String database)
            throws IOException, InterruptedException {
        return client.send(
                HttpRequest.newBuilder(URI.create(endpoint.replace("{databaseName}", database) + "/tx/" + txId))
                        .DELETE()
                        .build(),
                responseHandler());
    }

    public HttpResponse<QueryResponse> rollbackTx(String txId) throws IOException, InterruptedException {
        return rollbackTx(txId, "neo4j");
    }

    public void ensureServerIsAvailable() {
        assertEventually(
                () -> autoCommit(QueryRequest.newBuilder().statement("RETURN 1").build())
                                .statusCode()
                        == 202,
                available -> true,
                1,
                TimeUnit.MINUTES);
    }

    private HttpResponse<QueryResponse> sendRequest(QueryRequest request, String endpoint)
            throws IOException, InterruptedException {
        var reqBuilder = HttpRequest.newBuilder().uri(URI.create(endpoint));

        if (requiresTypedFormat) {
            reqBuilder
                    .header("Content-Type", "application/vnd.neo4j.query")
                    .header("Accept", "application/vnd.neo4j.query");
        } else {
            reqBuilder.header("Accept", "application/json").header("Content-Type", "application/json");
        }

        if (request != null) {
            reqBuilder.POST(requestPublisher(request));
        } else {
            reqBuilder.POST(HttpRequest.BodyPublishers.noBody());
        }

        if (credentials != null) {
            reqBuilder.header("Authorization", credentials);
        }

        var builtRequest = reqBuilder.build();
        return client.send(builtRequest, responseHandler());
    }

    private HttpResponse.BodyHandler<QueryResponse> responseHandler() {
        return responseInfo -> subscriberFrom((bytes) -> {
            if (bytes.length == 0) {
                return MAPPER.readValue("{}", QueryResponse.class);
            } else {
                return MAPPER.readValue(bytes, QueryResponse.class);
            }
        });
    }

    private HttpRequest.BodyPublisher requestPublisher(QueryRequest request) throws JsonProcessingException {
        return HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(request));
    }

    private <T> HttpResponse.BodySubscriber<T> subscriberFrom(IOFunction<byte[], T> ioFunction) {
        return HttpResponse.BodySubscribers.mapping(
                HttpResponse.BodySubscribers.ofByteArray(), rethrowRuntime(ioFunction));
    }

    @FunctionalInterface
    interface IOFunction<V, T> {
        T apply(V value) throws IOException;
    }

    private static <V, T> Function<V, T> rethrowRuntime(IOFunction<V, T> ioFunction) {
        return (V v) -> {
            try {
                return ioFunction.apply(v);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }
}
