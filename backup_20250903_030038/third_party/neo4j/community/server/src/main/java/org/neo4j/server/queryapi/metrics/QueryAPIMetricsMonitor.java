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
package org.neo4j.server.queryapi.metrics;

import org.eclipse.jetty.http.HttpVersion;

public interface QueryAPIMetricsMonitor {
    void totalRequests();

    void openTransaction();

    void closeTransaction();

    void totalTransactionsTimedOut();

    void requestTimeTaken(long timeInMillis);

    void requestContentType(String contentType);

    void responseContentType(String contentType);

    void responseStatusCode(int code);

    void httpVersion(HttpVersion httpVersion);

    void readRequest();

    void parameter();

    void beginRequest();

    void continueRequest();

    void commitRequest();

    void rollbackRequest();

    void autoCommitRequest();
}
