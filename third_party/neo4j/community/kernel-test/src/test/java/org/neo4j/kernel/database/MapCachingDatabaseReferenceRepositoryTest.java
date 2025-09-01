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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.DatabaseId.SYSTEM_DATABASE_ID;

import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class MapCachingDatabaseReferenceRepositoryTest {
    private final DatabaseReferenceRepository delegate = Mockito.mock(DatabaseReferenceRepository.class);
    private final UUID uuid = UUID.randomUUID();
    private final NamedDatabaseId dbId = DatabaseIdFactory.from("random", uuid);
    private final NormalizedDatabaseName name = new NormalizedDatabaseName(dbId.name());
    private final NormalizedDatabaseName aliasName = new NormalizedDatabaseName("foo");
    private final DatabaseReference ref = new DatabaseReferenceImpl.Internal(name, dbId, true);
    private final DatabaseReference aliasRef = new DatabaseReferenceImpl.Internal(aliasName, dbId, false);

    private DatabaseReferenceRepository.Caching databaseRefRepo;

    @BeforeEach
    void setUp() {
        when(delegate.getByAlias(aliasName)).thenReturn(Optional.of(aliasRef));
        when(delegate.getByAlias(name)).thenReturn(Optional.of(ref));
        when(delegate.getByUuid(uuid)).thenReturn(Optional.of(ref));
        databaseRefRepo = new MapCachingDatabaseReferenceRepository(delegate);
    }

    @Test
    void shouldLookupByName() {
        var lookup = databaseRefRepo.getByAlias(name);
        var lookupAlias = databaseRefRepo.getByAlias(aliasName);
        var lookupUnknown = databaseRefRepo.getByAlias(new NormalizedDatabaseName("unknown"));

        assertThat(lookup).contains(ref);
        assertThat(lookupAlias).contains(aliasRef);
        assertThat(lookupUnknown).isEmpty();
    }

    @Test
    void shouldLookupByUuid() {
        var lookup = databaseRefRepo.getByUuid(uuid);
        UUID unknown = UUID.randomUUID();

        // Very unlikely, but could still happen
        while (unknown == uuid || unknown == SYSTEM_DATABASE_ID.uuid()) {
            unknown = UUID.randomUUID();
        }

        var lookupUnknown = databaseRefRepo.getByUuid(unknown);

        assertThat(lookup).contains(ref);
        assertThat(lookupUnknown).isEmpty();
    }

    @Test
    void shouldCacheByByName() {
        var lookup = databaseRefRepo.getByAlias(name);
        var lookup2 = databaseRefRepo.getByAlias(name);
        var lookup3 = databaseRefRepo.getByUuid(uuid);

        assertThat(lookup).contains(ref);
        assertThat(lookup).isEqualTo(lookup2);
        assertThat(lookup).isEqualTo(lookup3);

        verify(delegate, atMostOnce()).getByAlias(name);
        verify(delegate, never()).getByUuid(uuid);
    }

    @Test
    void shouldCacheByByUuid() {
        var lookup = databaseRefRepo.getByUuid(uuid);
        var lookup2 = databaseRefRepo.getByUuid(uuid);
        var lookup3 = databaseRefRepo.getByAlias(name);

        assertThat(lookup).contains(ref);
        assertThat(lookup).isEqualTo(lookup2);
        assertThat(lookup).isEqualTo(lookup3);

        verify(delegate, atMostOnce()).getByUuid(uuid);
        verify(delegate, never()).getByAlias(name);
    }

    @Test
    void shouldNotCacheGetAllLookups() {
        databaseRefRepo.getAllDatabaseReferences();
        databaseRefRepo.getInternalDatabaseReferences();
        databaseRefRepo.getExternalDatabaseReferences();
        databaseRefRepo.getCompositeDatabaseReferences();
        databaseRefRepo.getAllDatabaseReferences();
        databaseRefRepo.getInternalDatabaseReferences();
        databaseRefRepo.getExternalDatabaseReferences();
        databaseRefRepo.getCompositeDatabaseReferences();

        verify(delegate, atLeast(2)).getAllDatabaseReferences();
        verify(delegate, atLeast(2)).getInternalDatabaseReferences();
        verify(delegate, atLeast(2)).getExternalDatabaseReferences();
        verify(delegate, atLeast(2)).getCompositeDatabaseReferences();
    }

    @Test
    void shouldIgnoreCase() {
        var lookup = databaseRefRepo.getByAlias(name.name().toLowerCase(Locale.ROOT));
        var lookup2 = databaseRefRepo.getByAlias(name.name().toUpperCase(Locale.ROOT));

        assertThat(lookup).contains(ref);
        assertThat(lookup).isEqualTo(lookup2);

        verify(delegate, atMostOnce()).getByAlias(name);
    }
}
