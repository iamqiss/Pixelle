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
package org.neo4j.internal.kernel.api.security;

import org.neo4j.kernel.database.DatabaseReference;

/**
 * Provides an RBAC interface to the database catalogue to make it easy to determine what
 * access a user has to a database. The logic is the same as is implemented for SHOW DATABASES.
 */
public interface DatabaseAccessMode {

    /**
     * Should the current user be able to know this database exists. The difference between visibility and access
     * is that DBA level permissions allow users to see all databases but not necessarily access them. All users
     * can see the system database.
     *
     * @param database a reference to the database
     * @return true if the user would be able to see the database in SHOW DATABASES, false otherwise
     */
    boolean canSeeDatabase(DatabaseReference database);

    /**
     * Is the current user able to access this database. All users have access to the system database.
     *
     * @param database a reference to the database
     * @return true if the user has access rights on this database, false otherwise
     */
    boolean canAccessDatabase(DatabaseReference database);

    DatabaseAccessMode FULL = new DatabaseAccessMode() {
        @Override
        public boolean canSeeDatabase(DatabaseReference database) {
            return true;
        }

        @Override
        public boolean canAccessDatabase(DatabaseReference database) {
            return true;
        }
    };
}
