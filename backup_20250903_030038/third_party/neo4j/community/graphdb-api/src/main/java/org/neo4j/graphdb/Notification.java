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
package org.neo4j.graphdb;

import org.neo4j.annotations.api.PublicApi;

/**
 * Representation for notifications found when executing a query.
 *
 * A notification can be visualized in a client pinpointing problems or other information about the query.
 * @deprecated replaced by {@link GqlStatusObject}
 */
@PublicApi
@Deprecated(forRemoval = true, since = "5.26")
public interface Notification {
    /**
     * Returns a notification code for the discovered issue.
     * @return the notification code
     * @deprecated replaced by {@link GqlStatusObject#gqlStatus()}
     */
    @Deprecated(forRemoval = true, since = "5.26")
    String getCode();

    /**
     * Returns a short summary of the notification.
     * @return the title of the notification.
     * @deprecated replaced by {@link GqlStatusObject#statusDescription()}
     */
    @Deprecated(forRemoval = true, since = "5.26")
    String getTitle();

    /**
     * Returns a longer description of the notification.
     * @return the description of the notification.
     * @deprecated replaced by {@link GqlStatusObject#statusDescription()}
     */
    @Deprecated(forRemoval = true, since = "5.26")
    String getDescription();

    /**
     * Returns the severity level of this notification.
     * @return the severity level of the notification.
     * @deprecated replaced by {@link GqlStatusObject#getSeverity()}
     */
    @Deprecated(forRemoval = true, since = "5.26")
    SeverityLevel getSeverity();

    /**
     * The position in the query where this notification points to.
     * Not all notifications have a unique position to point to and should in
     * that case return {@link org.neo4j.graphdb.InputPosition#empty}
     *
     * @return the position in the query where the issue was found, or
     * {@link org.neo4j.graphdb.InputPosition#empty} if no position is associated with this notification.
     * @deprecated replaced by {@link GqlStatusObject#getPosition()}
     */
    @Deprecated(forRemoval = true, since = "5.26")
    InputPosition getPosition();

    /**
     * Returns the category of this notification.
     * @return the category of the notification.
     * @deprecated replaced by {@link GqlStatusObject#getClassification()}
     */
    @Deprecated(forRemoval = true, since = "5.26")
    default NotificationCategory getCategory() {
        return NotificationCategory.UNKNOWN;
    }
}
