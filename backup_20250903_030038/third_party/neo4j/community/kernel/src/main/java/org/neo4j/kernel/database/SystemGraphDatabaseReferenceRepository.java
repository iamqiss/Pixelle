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
package org.neo4j.kernel.database;

import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;
import static org.neo4j.kernel.database.NamedDatabaseId.SYSTEM_DATABASE_NAME;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.systemgraph.CommunityTopologyGraphDbmsModel;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;
import org.neo4j.graphdb.DatabaseShutdownException;

public class SystemGraphDatabaseReferenceRepository implements DatabaseReferenceRepository {
    private static final DatabaseReference SYSTEM_DATABASE_REFERENCE = new DatabaseReferenceImpl.Internal(
            new NormalizedDatabaseName(SYSTEM_DATABASE_NAME), NAMED_SYSTEM_DATABASE_ID, true);

    private final Supplier<DatabaseContext> systemDatabaseSupplier;

    public SystemGraphDatabaseReferenceRepository(Supplier<DatabaseContext> systemDatabaseSupplier) {
        this.systemDatabaseSupplier = systemDatabaseSupplier;
    }

    @Override
    public Optional<DatabaseReference> getByAlias(NormalizedCatalogEntry catalogEntry) {
        if (catalogEntry.compositeDb().isEmpty() && catalogEntry.databaseAlias().equals(SYSTEM_DATABASE_NAME)) {
            return Optional.of(SYSTEM_DATABASE_REFERENCE);
        }
        return execute(model -> model.getDatabaseRefByAlias(catalogEntry));
    }

    @Override
    public Optional<DatabaseReference> getByAlias(NormalizedDatabaseName databaseAlias) {
        if (Objects.equals(SYSTEM_DATABASE_NAME, databaseAlias.name())) {
            return Optional.of(SYSTEM_DATABASE_REFERENCE);
        }

        return execute(model -> model.getDatabaseRefByAlias(normalizeCatalogName(databaseAlias.name())));
    }

    @Override
    public Optional<DatabaseReference> getByUuid(UUID databaseId) {
        if (Objects.equals(SYSTEM_DATABASE_REFERENCE.id(), databaseId)) {
            return Optional.of(SYSTEM_DATABASE_REFERENCE);
        }

        return execute(
                model -> model.getDatabaseIdByUUID(databaseId).flatMap(id -> model.getDatabaseRefByAlias(id.name())));
    }

    private String normalizeCatalogName(String databaseAlias) {
        // catalog has been quoted due to a .
        if (databaseAlias.matches("`.*\\..*`")) {
            String unquoted = databaseAlias.substring(1, databaseAlias.length() - 1);
            return unquoted.replace("``", "`");
        }
        return databaseAlias;
    }

    @Override
    public Set<DatabaseReference> getAllDatabaseReferences() {
        return execute(TopologyGraphDbmsModel::getAllDatabaseReferences);
    }

    @Override
    public Set<DatabaseReferenceImpl.Internal> getInternalDatabaseReferences() {
        return execute(TopologyGraphDbmsModel::getAllInternalDatabaseReferences);
    }

    @Override
    public Set<DatabaseReferenceImpl.External> getExternalDatabaseReferences() {
        return execute(TopologyGraphDbmsModel::getAllExternalDatabaseReferences);
    }

    @Override
    public Set<DatabaseReferenceImpl.Composite> getCompositeDatabaseReferences() {
        return execute(TopologyGraphDbmsModel::getAllCompositeDatabaseReferences);
    }

    private <T> T execute(Function<TopologyGraphDbmsModel, T> operation) {
        var databaseContext = systemDatabaseSupplier.get();
        var systemDb = databaseContext.databaseFacade();
        if (!systemDb.isAvailable(100)) {
            throw new DatabaseShutdownException(
                    (Throwable) new DatabaseManagementException("System database is not (yet) available"));
        }
        try (var tx = systemDb.beginTx()) {
            var model = new CommunityTopologyGraphDbmsModel(tx);
            return operation.apply(model);
        }
    }
}
