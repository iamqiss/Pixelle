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
package org.neo4j.procedure.impl.memory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.kernel.impl.core.RelationshipEntity;
import org.neo4j.kernel.impl.util.BaseCoreAPIPath;
import org.neo4j.memory.HeapEstimator;

public class ProcedureHeapEstimator implements org.neo4j.procedure.memory.HeapEstimator {
    public static final ProcedureHeapEstimator INSTANCE = new ProcedureHeapEstimator();

    private ProcedureHeapEstimator() {}

    @Override
    public long shallowSize(Object o) {
        return Estimations.shallowSize(o);
    }

    @Override
    public long shallowSizeOfInstance(Class<?> cls) {
        return HeapEstimator.shallowSizeOfInstance(cls);
    }

    @Override
    public long shallowSizeOfObjectArray(int size) {
        return HeapEstimator.shallowSizeOfObjectArray(size);
    }

    @Override
    public long sizeOfByteArray(int size) {
        return HeapEstimator.sizeOfByteArray(size);
    }

    @Override
    public long sizeOfIntArray(int size) {
        return HeapEstimator.sizeOfIntArray(size);
    }

    @Override
    public long sizeOfLongArray(int size) {
        return HeapEstimator.sizeOfLongArray(size);
    }

    @Override
    public long sizeOfFloatArray(int size) {
        return HeapEstimator.sizeOfFloatArray(size);
    }

    @Override
    public long sizeOfDoubleArray(int size) {
        return HeapEstimator.sizeOfDoubleArray(size);
    }

    public static class Estimations {
        private static final long SHALLOW_BASE_CORE_API_SIZE =
                HeapEstimator.shallowSizeOfInstance(BaseCoreAPIPath.class);

        private Estimations() {}

        public static long shallowSize(Object o) {
            // Intentionally ignores org.neo4j.memory.Measurable because it's not always a shallow size.
            if (o instanceof Boolean) return 0L;
            else if (o instanceof NodeEntity) return NodeEntity.SHALLOW_SIZE;
            else if (o instanceof RelationshipEntity) return RelationshipEntity.SHALLOW_SIZE;
            else if (o instanceof BaseCoreAPIPath path)
                return SHALLOW_BASE_CORE_API_SIZE + path.pathValue().estimatedHeapUsage();
            else if (o instanceof LocalDate) return HeapEstimator.LOCAL_DATE_SIZE;
            else if (o instanceof LocalDateTime) return HeapEstimator.LOCAL_DATE_TIME_SIZE;
            else if (o instanceof ZonedDateTime) return HeapEstimator.ZONED_DATE_TIME_SIZE;
            else if (o instanceof OffsetTime) return HeapEstimator.OFFSET_TIME_SIZE;
            else return HeapEstimator.sizeOf(o);
        }
    }
}
