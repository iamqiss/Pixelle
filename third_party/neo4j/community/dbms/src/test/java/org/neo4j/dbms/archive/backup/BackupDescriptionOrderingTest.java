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
package org.neo4j.dbms.archive.backup;

import static org.neo4j.logging.LogAssertions.assertThat;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.storageengine.api.StoreId;

public class BackupDescriptionOrderingTest {

    DatabaseId dbId1 = DatabaseIdFactory.from(new UUID(1, 0));
    DatabaseId dbId2 = DatabaseIdFactory.from(new UUID(2, 0));

    String dbName1 = "a";
    String dbName2 = "b";

    @Test
    void shouldOrderLexicographically() {

        // given
        var backup1 = createBackup(dbName1, dbId1, 1, 1, 10);
        var backup2 = createBackup(dbName1, dbId1, 1, 11, 12);
        var backup3 = createBackup(dbName1, dbId1, 1, 13, 20);

        var backup4 = createBackup(dbName1, dbId2, 1, 1, 20);
        var backup5 = createBackup(dbName1, dbId2, 1, 21, 30);

        var backup6 = createBackup(dbName2, dbId2, 1, 1, 5);
        var backup7 = createBackup(dbName2, dbId2, 1, 6, 15);

        var backups = Arrays.asList(backup4, backup2, backup5, backup7, backup6, backup3, backup1);

        // when
        Collections.sort(backups);

        // then
        var expected = List.of(backup1, backup2, backup3, backup4, backup5, backup6, backup7);
        assertThat(backups).containsExactlyElementsOf(expected);
    }

    @Test
    void shouldOrderByName() {

        // given
        var backup1 = createBackup(dbName1, dbId1, 1, 1, 10);
        var backup2 = createBackup(dbName2, dbId1, 1, 1, 10);

        var backups = Arrays.asList(backup2, backup1);

        // when
        Collections.sort(backups);

        // then
        var expected = List.of(backup1, backup2);
        assertThat(backups).containsExactlyElementsOf(expected);
    }

    @Test
    void shouldOrderByDbId() {

        // given
        var backup1 = createBackup(dbName1, dbId1, 1, 1, 10);
        var backup2 = createBackup(dbName1, dbId2, 1, 1, 10);

        var backups = Arrays.asList(backup2, backup1);

        // when
        Collections.sort(backups);

        // then
        var expected = List.of(backup1, backup2);
        assertThat(backups).containsExactlyElementsOf(expected);
    }

    @Test
    void shouldOrderByHighestTxId() {

        // given
        var backup1 = createBackup(dbName1, dbId1, 1, 1, 10);
        var backup2 = createBackup(dbName1, dbId1, 1, 1, 20);

        var backups = Arrays.asList(backup2, backup1);

        // when
        Collections.sort(backups);

        // then
        var expected = List.of(backup1, backup2);
        assertThat(backups).containsExactlyElementsOf(expected);
    }

    @Test
    void shouldOrderEmptyBackupByTime() {

        // given
        var backup1 = createBackup(dbName1, dbId1, 1, 0, 0);
        var backup2 = createBackup(dbName1, dbId1, 2, 1, 5);
        var backup3 = createBackup(dbName1, dbId1, 3, 0, 0);
        var backup4 = createBackup(dbName1, dbId1, 4, 6, 20);
        var backup5 = createBackup(dbName1, dbId1, 5, 0, 0);
        var backup6 = createBackup(dbName1, dbId1, 6, 0, 0);

        var backups = Arrays.asList(backup4, backup6, backup2, backup5, backup3, backup1);

        // when
        Collections.sort(backups);

        // then
        var expected = List.of(backup1, backup2, backup3, backup4, backup5, backup6);
        assertThat(backups).containsExactlyElementsOf(expected);
    }

    private BackupDescription createBackup(
            String databaseName,
            DatabaseId databaseId,
            long time,
            long lowestTransactionId,
            long highestTransactionId) {
        return new BackupDescription(
                databaseName,
                StoreId.UNKNOWN,
                databaseId,
                LocalDateTime.ofInstant(Instant.ofEpochSecond(time), ZoneId.systemDefault()),
                true,
                true,
                lowestTransactionId == 1,
                lowestTransactionId,
                highestTransactionId);
    }
}
