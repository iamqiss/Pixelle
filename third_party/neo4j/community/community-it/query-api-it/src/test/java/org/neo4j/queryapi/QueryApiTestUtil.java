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
package org.neo4j.queryapi;

import static org.assertj.core.api.Fail.fail;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.List;
import org.neo4j.collection.ResourceRawIterator;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.IntegralValue;

public final class QueryApiTestUtil {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static HttpRequest.Builder baseRequestBuilder(String endpoint, String databaseName) {
        return HttpRequest.newBuilder()
                .uri(URI.create(endpoint.replace("{databaseName}", databaseName)))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json");
    }

    public static HttpResponse<String> simpleRequest(
            HttpClient client, String endpoint, String database, String requestBody)
            throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(endpoint, database)
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        return client.send(httpRequest, HttpResponse.BodyHandlers.ofString());
    }

    public static HttpResponse<String> simpleRequest(HttpClient client, String endpoint, String requestBody)
            throws IOException, InterruptedException {
        return simpleRequest(client, endpoint, "neo4j", requestBody);
    }

    public static HttpResponse<String> simpleRequest(HttpClient client, String endpoint)
            throws IOException, InterruptedException {
        return simpleRequest(client, endpoint, "neo4j", "{\"statement\": \"RETURN 1\"}");
    }

    public static <T> T resolveDependency(DatabaseManagementService database, Class<T> cls) {
        return ((GraphDatabaseAPI) database.database("neo4j"))
                .getDependencyResolver()
                .resolveDependency(cls);
    }

    public static long getLastClosedTransactionId(DatabaseManagementService database) {
        var txIdStore = resolveDependency(database, TransactionIdStore.class);
        return txIdStore.getLastClosedTransactionId();
    }

    public static String encodedCredentials(String username, String password) {
        String valueToEncode = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(valueToEncode.getBytes());
    }

    public static void setupLogging() {
        try {
            Class<?> bridge = Class.forName("org.neo4j.server.logging.slf4j.SLF4JLogBridge");
            var log4jLogProvider = new Log4jLogProvider(System.out);
            Method setLogProvider = bridge.getMethod("setInstantiationContext", Log4jLogProvider.class, List.class);
            setLogProvider.invoke(null, log4jLogProvider, List.of("org.eclipse.jetty"));
        } catch (Exception e) {
            fail(String.format("Failed to set up jetty logging bridge: %s", e));
        }
    }

    public static CallableProcedure.BasicProcedure sleepProcedure() {
        return new CallableProcedure.BasicProcedure(procedureSignature(new QualifiedName("queryAPI", "nightnight"))
                .in("data", Neo4jTypes.NTInteger)
                .out(ProcedureSignature.VOID)
                .build()) {
            @Override
            public ResourceRawIterator<AnyValue[], ProcedureException> apply(
                    Context context, AnyValue[] objects, ResourceMonitor resourceMonitor) throws ProcedureException {
                try {
                    Thread.sleep(((IntegralValue) objects[0]).longValue());
                } catch (InterruptedException e) {
                    throw new ProcedureException(Status.General.UnknownError, e, "Interrupted");
                }
                return ResourceRawIterator.empty();
            }
        };
    }
}
