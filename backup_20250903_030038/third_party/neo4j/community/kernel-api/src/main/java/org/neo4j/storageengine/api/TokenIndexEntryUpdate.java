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
package org.neo4j.storageengine.api;

import java.util.Arrays;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.memory.HeapEstimator;

public class TokenIndexEntryUpdate<INDEX_KEY extends SchemaDescriptorSupplier> extends IndexEntryUpdate<INDEX_KEY> {
    private final int[] before;
    private final int[] values;
    private final boolean logical;

    TokenIndexEntryUpdate(long entityId, INDEX_KEY index_key, int[] before, int[] values, boolean logical) {
        super(entityId, index_key, UpdateMode.CHANGED);
        this.before = before;
        this.values = values;
        this.logical = logical;
    }

    public int[] values() {
        return values;
    }

    public int[] beforeValues() {
        if (before == null) {
            throw new UnsupportedOperationException("beforeValues is only valid for `UpdateMode.CHANGED");
        }
        return before;
    }

    public boolean isLogical() {
        return logical;
    }

    @Override
    public long roughSizeOfUpdate() {
        return HeapEstimator.sizeOf(values) + (updateMode() == UpdateMode.CHANGED ? HeapEstimator.sizeOf(before) : 0);
    }

    @Override
    protected boolean valueEquals(IndexEntryUpdate<?> o) {
        if (!(o instanceof TokenIndexEntryUpdate<?> that)) {
            return false;
        }
        if (!Arrays.equals(before, that.before)) {
            return false;
        }
        return Arrays.equals(values, that.values);
    }

    @Override
    protected int valueHash() {
        int result = Arrays.hashCode(before);
        result = 31 * result + Arrays.hashCode(values);
        return result;
    }

    @Override
    protected String valueToString() {
        return String.format("beforeValues=%s, values=%s", Arrays.toString(before), Arrays.toString(values));
    }
}
