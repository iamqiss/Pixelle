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
package org.neo4j.shell.completions;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.neo4j.driver.Record;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.state.BoltStateHandler;

public class QueryPoller implements AutoCloseable {
    private static final Logger log = Logger.create();
    private final ScheduledExecutorService poller = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> pollingThread;
    Runnable pollingWorkload;
    private static final int ITEM_LIMIT = 1000;
    private final BoltStateHandler boltStateHandler;

    public static String fetchDataSummary =
            "CALL db.labels() YIELD label\n" + String.format("RETURN COLLECT(label)[..%s] AS result\n", ITEM_LIMIT)
                    + "UNION ALL\n"
                    + "CALL db.relationshipTypes() YIELD relationshipType\n"
                    + String.format("RETURN COLLECT(relationshipType)[..%s] AS result\n", ITEM_LIMIT)
                    + "UNION ALL\n"
                    + "CALL db.propertyKeys() YIELD propertyKey\n"
                    + String.format("RETURN COLLECT(propertyKey)[..%s] AS result", ITEM_LIMIT);

    public static String fetchProcedures = "SHOW PROCEDURES YIELD name";
    public static String fetchFunctions = "SHOW FUNCTIONS YIELD name";
    public static String fetchDatabases = "SHOW DATABASES YIELD name, aliases;";
    public static String fetchRoles = "SHOW ROLES YIELD role;";
    public static String fetchUsers = "SHOW USERS YIELD user;";

    public QueryPoller(BoltStateHandler boltStateHandler) {
        this.boltStateHandler = boltStateHandler;
    }

    @Override
    public void close() throws Exception {
        pollingThread.cancel(true);
        poller.shutdown();
    }

    public record PollingQuery(String query, Consumer<List<Record>> onFetch) {}

    public void startPolling(PollingQuery... queries) {
        pollingWorkload = () -> {
            for (PollingQuery q : queries) {
                try {
                    if (boltStateHandler != null && boltStateHandler.isConnected()) {
                        var result = boltStateHandler.runServiceCypher(q.query, Map.of());
                        if (result.isPresent()) {
                            var records = result.get().getRecords();
                            q.onFetch.accept(records);
                        }
                    }
                } catch (Exception e) {
                    log.warn("Failed to fetch auto completion metadata with query: " + q.query, e);
                }
            }
        };
        pollingThread = poller.scheduleWithFixedDelay(pollingWorkload, 5, 30, TimeUnit.SECONDS);
    }

    public void resumePolling() {
        if (pollingWorkload != null && pollingThread == null) {
            pollingThread = poller.scheduleWithFixedDelay(pollingWorkload, 5, 30, TimeUnit.SECONDS);
        }
    }

    public void stopPolling() {
        if (pollingThread != null) {
            pollingThread.cancel(false);
            pollingThread = null;
        }
    }
}
