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
package org.neo4j.internal.recordstorage.indexcommand;

import org.neo4j.internal.recordstorage.Command;
import org.neo4j.internal.recordstorage.LogCommandSerialization;
import org.neo4j.lock.LockGroup;
import org.neo4j.lock.LockService;
import org.neo4j.lock.LockType;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.UpdateMode;

public abstract class IndexUpdateCommand<T> extends Command {
    final UpdateMode updateMode;
    final long indexId;
    final long entityId;

    public IndexUpdateCommand(
            LogCommandSerialization serialization, UpdateMode updateMode, long indexId, long entityId) {
        super(serialization);
        this.updateMode = updateMode;
        this.indexId = indexId;
        this.entityId = entityId;
    }

    public UpdateMode getUpdateMode() {
        return updateMode;
    }

    public long getIndexId() {
        return indexId;
    }

    public long getEntityId() {
        return entityId;
    }

    public abstract T getBefore();

    public abstract T getAfter();

    @Override
    public void lockForRecovery(LockService lockService, LockGroup lockGroup, TransactionApplicationMode mode) {
        lockGroup.add(lockService.acquireCustomLock(RECOVERY_LOCK_TYPE_SCHEMA_RULE, getIndexId(), LockType.EXCLUSIVE));
    }
}
