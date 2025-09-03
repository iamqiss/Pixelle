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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * contains normalized catalog entries consisting of an optional composite name (for constituents)
 * plus a database alias
 * [compositeDb, databaseAlias] for constituents
 * [null, databaseAlias] for database names & aliases
 */
public class NormalizedCatalogEntry {
    private final Optional<String> compositeDb;
    private final String databaseAlias;

    public NormalizedCatalogEntry(String compositeDb, String databaseAlias) {
        this.compositeDb = Optional.of(compositeDb.toLowerCase());
        this.databaseAlias = databaseAlias.toLowerCase();
    }

    public NormalizedCatalogEntry(String databaseAlias) {
        this.compositeDb = Optional.empty();
        this.databaseAlias = databaseAlias.toLowerCase();
    }

    public static NormalizedCatalogEntry fromList(List<String> nameParts) {
        if (nameParts.size() == 2) {
            return new NormalizedCatalogEntry(nameParts.get(0), nameParts.get(1));
        } else if (nameParts.size() == 1) {
            return new NormalizedCatalogEntry(nameParts.get(0));
        }

        throw new IllegalStateException("a database alias should not consist of more than 2 parts");
    }

    public Optional<String> compositeDb() {
        return compositeDb;
    }

    public String databaseAlias() {
        return databaseAlias;
    }

    public String stringRepresentation() {
        return compositeDb.map(s -> s + "." + databaseAlias).orElse(databaseAlias);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        return obj instanceof NormalizedCatalogEntry other
                && Objects.equals(compositeDb, other.compositeDb)
                && Objects.equals(databaseAlias, other.databaseAlias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(compositeDb, databaseAlias);
    }
}
