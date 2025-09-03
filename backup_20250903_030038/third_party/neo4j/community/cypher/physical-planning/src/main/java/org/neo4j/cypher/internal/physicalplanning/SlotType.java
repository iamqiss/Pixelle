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

/**
 * The slot types are an optimisation meant to replace cached lambda functions,
 * that were occupying lots of heap memory in the query cache.
 */
public final class SlotType {

    public static final int NodeNonNullLongSlot = 1;
    public static final int NodeNullableLongSlot = 2;
    public static final int RelNonNullLongSlot = 3;
    public static final int RelNullableLongSlot = 4;
    public static final int OtherNonNullLongSlot = 5;
    public static final int OtherNullableLongSlot = 6;
    public static final int NodeNonNullRefSlot = 7;
    public static final int NodeNullableRefSlot = 8;
    public static final int RelNonNullRefSlot = 9;
    public static final int RelNullableRefSlot = 10;
    public static final int OtherNonNullRefSlot = 11;
    public static final int OtherNullableRefSlot = 12;

    private SlotType() {}

    public static boolean isValidSlotType(int slotType) {
        return slotType >= NodeNonNullLongSlot && slotType <= OtherNullableRefSlot;
    }

    public static boolean isNullable(int slotType) {
        assert isValidSlotType(slotType);
        return slotType % 2 == 0;
    }

    public static boolean isLongSlot(int slotType) {
        assert isValidSlotType(slotType);
        return slotType <= OtherNullableLongSlot;
    }

    public static boolean isRefSlot(int slotType) {
        assert isValidSlotType(slotType);
        return slotType >= NodeNonNullRefSlot;
    }
}
