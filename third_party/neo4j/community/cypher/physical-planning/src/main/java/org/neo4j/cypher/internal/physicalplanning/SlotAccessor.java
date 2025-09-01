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
package org.neo4j.cypher.internal.physicalplanning;

import static org.neo4j.cypher.internal.physicalplanning.SlotType.NodeNonNullLongSlot;
import static org.neo4j.cypher.internal.physicalplanning.SlotType.NodeNonNullRefSlot;
import static org.neo4j.cypher.internal.physicalplanning.SlotType.NodeNullableLongSlot;
import static org.neo4j.cypher.internal.physicalplanning.SlotType.NodeNullableRefSlot;
import static org.neo4j.cypher.internal.physicalplanning.SlotType.OtherNonNullLongSlot;
import static org.neo4j.cypher.internal.physicalplanning.SlotType.OtherNonNullRefSlot;
import static org.neo4j.cypher.internal.physicalplanning.SlotType.OtherNullableLongSlot;
import static org.neo4j.cypher.internal.physicalplanning.SlotType.OtherNullableRefSlot;
import static org.neo4j.cypher.internal.physicalplanning.SlotType.RelNonNullLongSlot;
import static org.neo4j.cypher.internal.physicalplanning.SlotType.RelNonNullRefSlot;
import static org.neo4j.cypher.internal.physicalplanning.SlotType.RelNullableLongSlot;
import static org.neo4j.cypher.internal.physicalplanning.SlotType.RelNullableRefSlot;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.virtual.VirtualValues.node;
import static org.neo4j.values.virtual.VirtualValues.relationship;

import org.neo4j.cypher.internal.runtime.CypherRow;
import org.neo4j.cypher.internal.runtime.ReadableRow;
import org.neo4j.cypher.internal.runtime.WritableRow;
import org.neo4j.exceptions.InternalException;
import org.neo4j.exceptions.ParameterWrongTypeException;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

public class SlotAccessor {
    private SlotAccessor() {}

    private static final int PRIMITIVE_NULL = -1;

    public static AnyValue getValue(ReadableRow row, SlotConfiguration.KeyedSlot slot) {
        final var offset = slot.offset();
        return switch (slot.slotType()) {
            case NodeNonNullLongSlot -> node(row.getLongAt(offset));
            case NodeNullableLongSlot -> nullableNode(row.getLongAt(offset));
            case RelNonNullLongSlot -> relationship(row.getLongAt(offset));
            case RelNullableLongSlot -> nullableRel(row.getLongAt(offset));
            case OtherNonNullLongSlot, OtherNullableLongSlot -> throw failedToMakeGetter(slot.slot());
            case NodeNonNullRefSlot,
                    NodeNullableRefSlot,
                    RelNonNullRefSlot,
                    RelNullableRefSlot,
                    OtherNonNullRefSlot,
                    OtherNullableRefSlot -> row.getRefAt(offset);
            default -> throw new InternalException("Unknown slot type " + slot.slotType());
        };
    }

    public static void setValue(WritableRow row, SlotConfiguration.KeyedSlot slot, AnyValue value) {
        final var offset = slot.offset();
        switch (slot.slotType()) {
            case NodeNonNullLongSlot -> {
                if (value instanceof VirtualNodeValue n) row.setLongAt(offset, n.id());
                else throw wrongType("node", slot.slot(), value);
            }
            case NodeNullableLongSlot -> {
                if (value instanceof VirtualNodeValue n) row.setLongAt(offset, n.id());
                else if (value == NO_VALUE) row.setLongAt(offset, PRIMITIVE_NULL);
                else throw wrongType("node", slot.slot(), value);
            }
            case RelNonNullLongSlot -> {
                if (value instanceof VirtualRelationshipValue n) row.setLongAt(offset, n.id());
                else throw wrongType("relationship", slot.slot(), value);
            }
            case RelNullableLongSlot -> {
                if (value instanceof VirtualRelationshipValue n) row.setLongAt(offset, n.id());
                else if (value == NO_VALUE) row.setLongAt(offset, PRIMITIVE_NULL);
                else throw wrongType("relationship", slot.slot(), value);
            }
            case OtherNonNullLongSlot, OtherNullableLongSlot -> throw failedToMakeSetter(slot.slot());
            case NodeNonNullRefSlot,
                    NodeNullableRefSlot,
                    RelNonNullRefSlot,
                    RelNullableRefSlot,
                    OtherNonNullRefSlot,
                    OtherNullableRefSlot -> row.setRefAt(offset, value);
            default -> throw new InternalException("Unknown slot type " + slot.slotType());
        }
    }

    public static void invalidateCachedNodeProperties(CypherRow row, SlotConfiguration slots, long node) {
        final var allSlots = slots.slots();
        final var end = slots.cachedPropertiesEndIndex();
        for (int i = slots.cachedPropertiesStartIndex(); i < end; i++) {
            final var cachedSlot = allSlots.apply(i);
            final var entityName = ((SlotConfiguration.CachedPropertySlotKey) cachedSlot.key())
                    .property()
                    .entityName();
            final var entitySlot = slots.getOrElse(new SlotConfiguration.VariableSlotKey(entityName), null);
            // It is possible that entitySlot is null, when we allocate a cached property before a pipeline break and
            // before the variable it is referencing.
            // We will never evaluate that cached property in this row, and we could improve SlotAllocation to allocate
            // it only on the next pipeline
            // instead, but that is difficult. It is harmless if we get here, we will simply not do anything.
            if (entitySlot != null && isNode(row, entitySlot, node)) {
                row.setCachedPropertyAt(cachedSlot.offset(), null);
            }
        }
    }

    public static void invalidateCachedRelProperties(CypherRow row, SlotConfiguration slots, long node) {
        final var allSlots = slots.slots();
        final var end = slots.cachedPropertiesEndIndex();
        for (int i = slots.cachedPropertiesStartIndex(); i < end; i++) {
            final var cachedSlot = allSlots.apply(i);
            final var entityName = ((SlotConfiguration.CachedPropertySlotKey) cachedSlot.key())
                    .property()
                    .entityName();
            final var entitySlot = slots.getOrElse(new SlotConfiguration.VariableSlotKey(entityName), null);
            // It is possible that entitySlot is null, when we allocate a cached property before a pipeline break and
            // before the variable it is referencing.
            // We will never evaluate that cached property in this row, and we could improve SlotAllocation to allocate
            // it only on the next pipeline
            // instead, but that is difficult. It is harmless if we get here, we will simply not do anything.
            if (entitySlot != null && isRel(row, entitySlot, node)) {
                row.setCachedPropertyAt(cachedSlot.offset(), null);
            }
        }
    }

    private static boolean isNode(CypherRow row, SlotConfiguration.KeyedSlot slot, long node) {
        return switch (slot.slotType()) {
            case NodeNonNullLongSlot, NodeNullableLongSlot -> row.getLongAt(slot.offset()) == node;
            case NodeNonNullRefSlot, NodeNullableRefSlot -> row.getRefAt(slot.offset()) instanceof VirtualNodeValue n
                    && n.id() == node;
            default -> false; // Note! The slot can contain a node here too, but this mimics previous behaviour
        };
    }

    private static boolean isRel(CypherRow row, SlotConfiguration.KeyedSlot slot, long rel) {
        return switch (slot.slotType()) {
            case RelNonNullLongSlot, RelNullableLongSlot -> row.getLongAt(slot.offset()) == rel;
            case RelNonNullRefSlot, RelNullableRefSlot -> row.getRefAt(slot.offset())
                            instanceof VirtualRelationshipValue n
                    && n.id() == rel;
            default -> false; // Note! The slot can contain a relationship here too, but this mimics previous behaviour
        };
    }

    public static AnyValue nullableNode(long id) {
        return id == PRIMITIVE_NULL ? NO_VALUE : node(id);
    }

    public static AnyValue nullableRel(long id) {
        return id == PRIMITIVE_NULL ? NO_VALUE : relationship(id);
    }

    private static ParameterWrongTypeException wrongType(String expected, Slot slot, Object actual) {
        return new ParameterWrongTypeException("Expected to find a %s at %s slot %s but found %s instead"
                .formatted(expected, slot.isLongSlot() ? "long" : "ref", slot.offset(), actual));
    }

    private static InternalException failedToMakeGetter(Slot slot) {
        return new InternalException("Do not know how to make getter for slot " + slot);
    }

    private static InternalException failedToMakeSetter(Slot slot) {
        return new InternalException("Do not know how to make setter for slot " + slot);
    }
}
