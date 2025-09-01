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
package org.neo4j.fabric.stream.summary;

import static org.neo4j.notifications.StandardGqlStatusObject.isStandardGqlStatusCode;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GqlStatusObject;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryStatistics;
import reactor.core.publisher.Mono;

public class MergedSummary implements Summary {
    private final MergedQueryStatistics statistics;
    private final Set<Notification> notifications;
    private final Set<GqlStatusObject> gqlStatusObjects;
    private final AtomicReference<Collection<GqlStatusObject>> lastUpdatedGqlStatusObjects;
    private Mono<ExecutionPlanDescription> executionPlanDescription;

    public MergedSummary(
            Mono<ExecutionPlanDescription> executionPlanDescription,
            MergedQueryStatistics statistics,
            Set<Notification> notifications,
            Set<GqlStatusObject> gqlStatusObjects,
            AtomicReference<Collection<GqlStatusObject>> lastUpdatedGqlStatusObjects) {
        this.executionPlanDescription = executionPlanDescription;
        this.statistics = statistics;
        this.notifications = notifications;
        this.gqlStatusObjects = gqlStatusObjects;
        this.lastUpdatedGqlStatusObjects = lastUpdatedGqlStatusObjects;
    }

    @Override
    public ExecutionPlanDescription executionPlanDescription() {
        return executionPlanDescription.cache().block();
    }

    @Override
    public Collection<Notification> getNotifications() {
        return notifications;
    }

    @Override
    public Collection<GqlStatusObject> getGqlStatusObjects() {
        // Want to only keep the "standard" gql status from the last set of objects added
        // so remove all standard statuses and then add the last statuses to the set again.
        if (lastUpdatedGqlStatusObjects.get() != null) {
            gqlStatusObjects.removeIf(gso -> isStandardGqlStatusCode(gso.gqlStatus()));
            gqlStatusObjects.addAll(lastUpdatedGqlStatusObjects.get());
        }

        return gqlStatusObjects;
    }

    @Override
    public QueryStatistics getQueryStatistics() {
        return statistics;
    }
}
