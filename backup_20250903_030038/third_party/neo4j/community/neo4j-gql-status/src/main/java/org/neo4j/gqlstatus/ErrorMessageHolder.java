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
package org.neo4j.gqlstatus;

public class ErrorMessageHolder {

    // Adding a real feature flag in GraphDatabaseInternalSettings would cause a circle dependency,
    // so we need to settle for a global variable
    public static boolean USE_NEW_ERROR_MESSAGES = false;

    public static String getMessage(ErrorGqlStatusObject gqlStatusObject, String oldMessage) {
        if (gqlStatusObject instanceof ErrorGqlStatusObjectImplementation gso && gso.isCause()
                || USE_NEW_ERROR_MESSAGES) {
            return gqlStatusObject.getMessage();
        }
        // if this is a top-level error we need to send the old error message for backwards-compatibility
        return oldMessage;
    }

    public static String getOldCauseMessage(Throwable cause) {
        if (cause instanceof ErrorGqlStatusObject gqlCause) {
            return gqlCause.legacyMessage();
        } else {
            return cause.getMessage();
        }
    }
}
