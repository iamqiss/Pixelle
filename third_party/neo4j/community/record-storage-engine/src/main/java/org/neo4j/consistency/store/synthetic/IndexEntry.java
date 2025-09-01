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
package org.neo4j.consistency.store.synthetic;

import static java.lang.String.format;

import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.string.Mask;

/**
 * Synthetic record type that stands in for a real record to fit in conveniently
 * with consistency checking
 */
public class IndexEntry extends AbstractBaseRecord {
    private final IndexDescriptor indexDescriptor;
    private final TokenNameLookup tokenNameLookup;

    public IndexEntry(IndexDescriptor indexDescriptor, TokenNameLookup tokenNameLookup, long nodeId) {
        super(nodeId);
        this.indexDescriptor = indexDescriptor;
        this.tokenNameLookup = tokenNameLookup;
        setInUse(true);
    }

    @Override
    public void clear() {
        initialize(false);
    }

    @Override
    public String toString(Mask mask) {
        return format(
                "IndexEntry[%s=%d, index=%s]",
                indexDescriptor.schema().entityType() == EntityType.NODE ? "nodeId" : "relationshipId",
                getId(),
                indexDescriptor.userDescription(tokenNameLookup));
    }
}
