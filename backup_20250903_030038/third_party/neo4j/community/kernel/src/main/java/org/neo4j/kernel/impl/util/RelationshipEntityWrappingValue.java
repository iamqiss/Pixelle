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
package org.neo4j.kernel.impl.util;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.values.AnyValueWriter.EntityMode.REFERENCE;

import java.util.function.Consumer;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.kernel.impl.core.RelationshipEntity;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.RelationshipVisitor;
import org.neo4j.values.virtual.VirtualNodeReference;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualValues;

public class RelationshipEntityWrappingValue extends RelationshipValue implements WrappingEntity<Relationship> {
    static final long SHALLOW_SIZE =
            shallowSizeOfInstance(RelationshipEntityWrappingValue.class) + RelationshipEntity.SHALLOW_SIZE;

    private final Relationship relationship;
    private volatile TextValue type;
    private volatile MapValue properties;
    private volatile VirtualNodeReference startNode;
    private volatile VirtualNodeReference endNode;

    /**
     * Wraps a {@link Relationship}, reading out its meta data lazily, i.e. on first access of e.g. {@link #startNodeId()}.
     */
    static RelationshipEntityWrappingValue wrapLazy(Relationship relationship) {
        return new RelationshipEntityWrappingValue(relationship);
    }

    private RelationshipEntityWrappingValue(Relationship relationship) {
        super(relationship.getId(), LongReference.NULL, LongReference.NULL);
        this.relationship = relationship;
    }

    public Relationship relationshipEntity() {
        return relationship;
    }

    @Override
    public <E extends Exception> void writeTo(AnyValueWriter<E> writer) throws E {
        if (writer.entityMode() == REFERENCE) {
            writer.writeRelationshipReference(id());
        } else {
            boolean isDeleted = false;

            if (relationship instanceof RelationshipEntity proxy) {
                if (!proxy.initializeData()) {
                    // If the relationship has been deleted since it was found by the query,
                    // then we'll have to tell the client that their transaction conflicted,
                    // and that they need to retry it.
                    var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25N11)
                            .build();
                    throw new ReadAndDeleteTransactionConflictException(
                            gql, RelationshipEntity.isDeletedInCurrentTransaction(relationship));
                }
            }

            MapValue p;
            try {
                p = properties();
            } catch (ReadAndDeleteTransactionConflictException e) {
                if (!e.wasDeletedInThisTransaction()) {
                    throw e;
                }
                // If it isn't a transient error then the relationship was deleted in the current transaction and we
                // should write an 'empty' relationship.
                p = VirtualValues.EMPTY_MAP;
                isDeleted = true;
            }

            if (id() < 0) {
                writer.writeVirtualRelationshipHack(relationship);
            }
            writer.writeRelationship(
                    elementId(),
                    id(),
                    startNode().elementId(),
                    startNode().id(),
                    endNode().elementId(),
                    endNode().id(),
                    type(),
                    p,
                    isDeleted);
        }
    }

    @Override
    public long estimatedHeapUsage() {
        long size = SHALLOW_SIZE;
        if (type != null) {
            size += type.estimatedHeapUsage();
        }
        if (properties != null) {
            size += properties.estimatedHeapUsage();
        }
        if (startNode != null) {
            size += startNode.estimatedHeapUsage();
        }
        if (endNode != null) {
            size += endNode.estimatedHeapUsage();
        }
        return size;
    }

    public void populate(RelationshipScanCursor relCursor, PropertyCursor propertyCursor) {
        try {
            if (relationship instanceof RelationshipEntity proxy) {
                if (!proxy.initializeData(relCursor)) {
                    // When this happens to relationship proxies, we have most likely observed our relationship being
                    // deleted by an overlapping committed
                    // transaction.
                    return;
                }
            }
            // type, startNode and endNode will have counted their DB hits as part of initializeData.
            type();
            properties(propertyCursor);
            startNode();
            endNode();
        } catch (NotFoundException | ReadAndDeleteTransactionConflictException e) {
            // best effort, cannot do more
        }
    }

    public void populate() {
        try {
            if (relationship instanceof RelationshipEntity proxy) {
                if (!proxy.initializeData()) {
                    // When this happens to relationship proxies, we have most likely observed our relationship being
                    // deleted by an overlapping committed
                    // transaction.
                    return;
                }
            }
            type();
            properties();
            startNode();
            endNode();
        } catch (NotFoundException e) {
            if (!RelationshipEntity.isDeletedInCurrentTransaction(relationship)) {
                throw e;
            }
            // best effort, cannot do more
        } catch (ReadAndDeleteTransactionConflictException e) {
            if (!e.wasDeletedInThisTransaction()) {
                throw e;
            }
            // best effort, cannot do more
        }
    }

    public boolean isPopulated() {
        return type != null && properties != null && startNode != null && endNode != null;
    }

    public boolean canPopulate() {
        if (relationship instanceof RelationshipEntity entity) {
            return entity.getTransaction().isOpen();
        }
        return true;
    }

    @Override
    public long startNodeId(Consumer<RelationshipVisitor> consumer) {
        long startNodeId = super.startNodeId(consumer);
        return startNodeId != LongReference.NULL ? startNodeId : startNode().id();
    }

    @Override
    public long endNodeId(Consumer<RelationshipVisitor> consumer) {
        long endNodeId = super.endNodeId(consumer);
        return endNodeId != LongReference.NULL ? endNodeId : endNode().id();
    }

    @Override
    public long startNodeId() {
        // Often a RelationshipEntityWrappingValue is initialized with the start/end node ids given, but if that's not
        // the case
        // Then use the other route of looking up the start node the slow way and getting its ID.
        long startNodeId = super.startNodeId();
        return startNodeId != LongReference.NULL ? startNodeId : startNode().id();
    }

    @Override
    public long endNodeId() {
        // Often a RelationshipEntityWrappingValue is initialized with the start/end node ids given, but if that's not
        // the case
        // Then use the other route of looking up the end node the slow way and getting its ID.
        long endNodeId = super.endNodeId();
        return endNodeId != LongReference.NULL ? endNodeId : endNode().id();
    }

    @Override
    public VirtualNodeReference startNode() {
        var start = startNode;
        if (start == null) {
            synchronized (this) {
                start = startNode;
                if (start == null) {
                    start = startNode = ValueUtils.asNodeReference(relationship.getStartNode());
                }
            }
        }
        return start;
    }

    @Override
    public VirtualNodeReference endNode() {
        var end = endNode;
        if (end == null) {
            synchronized (this) {
                end = endNode;
                if (end == null) {
                    end = endNode = ValueUtils.asNodeReference(relationship.getEndNode());
                }
            }
        }
        return end;
    }

    @Override
    public VirtualNodeValue otherNode(VirtualNodeValue node) {
        if (node instanceof NodeEntityWrappingNodeValue) {
            Node proxy = ((NodeEntityWrappingNodeValue) node).getEntity();
            return ValueUtils.fromNodeEntity(relationship.getOtherNode(proxy));
        } else {
            return super.otherNode(node);
        }
    }

    @Override
    public long otherNodeId(long node) {
        return relationship.getOtherNodeId(node);
    }

    @Override
    public TextValue type() {
        TextValue t = type;
        if (t == null) {
            try {
                synchronized (this) {
                    t = type;
                    if (t == null) {
                        t = type = Values.utf8Value(relationship.getType().name());
                    }
                }
            } catch (IllegalStateException e) {
                var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25N11)
                        .build();
                throw new ReadAndDeleteTransactionConflictException(
                        gql, RelationshipEntity.isDeletedInCurrentTransaction(relationship), e);
            }
        }
        return t;
    }

    @Override
    public MapValue properties() {
        MapValue m = properties;
        if (m == null) {
            try {
                synchronized (this) {
                    m = properties;
                    if (m == null) {
                        m = properties = ValueUtils.asMapValue(relationship.getAllProperties());
                    }
                }
            } catch (NotFoundException | IllegalStateException e) {
                var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25N11)
                        .build();
                throw new ReadAndDeleteTransactionConflictException(
                        gql, RelationshipEntity.isDeletedInCurrentTransaction(relationship), e);
            }
        }
        return m;
    }

    @Override
    public String elementId() {
        return relationship.getElementId();
    }

    @Override
    public Relationship getEntity() {
        return relationship;
    }

    public MapValue properties(PropertyCursor propertyCursor) {
        MapValue m = properties;
        if (m == null) {
            try {
                synchronized (this) {
                    m = properties;
                    if (m == null) {
                        var relProperties = relationship instanceof RelationshipEntity
                                ? ((RelationshipEntity) relationship).getAllProperties(propertyCursor)
                                : relationship.getAllProperties();
                        m = properties = ValueUtils.asMapValue(relProperties);
                    }
                }
            } catch (NotFoundException | IllegalStateException e) {
                var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25N11)
                        .build();
                throw new ReadAndDeleteTransactionConflictException(
                        gql, RelationshipEntity.isDeletedInCurrentTransaction(relationship), e);
            }
        }
        return m;
    }
}
