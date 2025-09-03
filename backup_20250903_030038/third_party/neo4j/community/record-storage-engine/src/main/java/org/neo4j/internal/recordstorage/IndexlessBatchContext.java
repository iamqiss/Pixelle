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
package org.neo4j.internal.recordstorage;

import org.neo4j.io.IOUtils;
import org.neo4j.lock.LockGroup;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.util.IdUpdateListener;

public class IndexlessBatchContext implements BatchContext {
    private final LockGroup lockGroup = new LockGroup();
    private final IndexActivator indexActivator;
    private final IdUpdateListener idUpdateListener;
    private final MemoryTracker memoryTracker;

    public IndexlessBatchContext(
            IndexUpdateListener indexUpdateListener, IdUpdateListener idUpdateListener, MemoryTracker memoryTracker) {
        this.indexActivator = new IndexActivator(indexUpdateListener);
        this.idUpdateListener = idUpdateListener;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public LockGroup getLockGroup() {
        return lockGroup;
    }

    @Override
    public IndexActivator getIndexActivator() {
        return indexActivator;
    }

    @Override
    public void applyPendingIndexUpdates() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexUpdates indexUpdates() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IdUpdateListener getIdUpdateListener() {
        return idUpdateListener;
    }

    @Override
    public MemoryTracker memoryTracker() {
        return memoryTracker;
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeAll(lockGroup, indexActivator, idUpdateListener);
    }
}
