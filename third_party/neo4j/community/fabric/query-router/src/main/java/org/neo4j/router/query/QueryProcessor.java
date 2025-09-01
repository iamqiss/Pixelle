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
package org.neo4j.router.query;

import java.util.Optional;
import java.util.Set;
import org.neo4j.cypher.internal.QueryOptions;
import org.neo4j.cypher.internal.util.CancellationChecker;
import org.neo4j.cypher.internal.util.InternalNotification;
import org.neo4j.cypher.internal.util.ObfuscationMetadata;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.router.impl.query.StatementType;
import org.neo4j.router.location.LocationService;

/**
 * Parse a query and extract all information interesting for Query Router from it.
 * It also might rewrite the query, so the query returned as result should be used
 * in the steps after this one.
 */
public interface QueryProcessor {

    record ProcessedQueryInfo(
            DatabaseReference target,
            Query rewrittenQuery,
            Optional<ObfuscationMetadata> obfuscationMetadata,
            StatementType statementType,
            QueryOptions queryOptions,
            Set<InternalNotification> parsingNotifications,
            Set<InternalNotification> routingNotifications) {}

    ProcessedQueryInfo processQuery(
            Query query,
            TargetService targetService,
            LocationService locationService,
            CancellationChecker cancellationChecker,
            DatabaseReference sessionDatabase);

    long clearQueryCachesForDatabase(String databaseName);

    DatabaseContextProvider<?> databaseContextProvider();
}
