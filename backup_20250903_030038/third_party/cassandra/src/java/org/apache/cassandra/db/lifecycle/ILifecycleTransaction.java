/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.lifecycle;

import java.util.Collection;
import java.util.Set;

import com.google.common.collect.Iterables;

import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Transactional;

public interface ILifecycleTransaction extends Transactional
{
    void trackNew(SSTable sstable);
    void untrackNew(SSTable sstable);
    OperationType opType();
    void checkpoint();
    void update(SSTableReader reader, boolean original);
    void update(Collection<SSTableReader> readers, boolean original);
    SSTableReader current(SSTableReader reader);
    void obsolete(SSTableReader reader);
    void obsoleteOriginals();
    Set<SSTableReader> originals();
    boolean isObsolete(SSTableReader reader);
    boolean isOffline();
    TimeUUID opId();

    /// Op identifier as a string to use in debug prints. Usually just the opId, with added part information for partial
    /// transactions.
    default String opIdString()
    {
        return opId().toString();
    }

    void cancel(SSTableReader removedSSTable);

    default void abort()
    {
        Throwables.maybeFail(abort(null));
    }

    default void commit()
    {
        Throwables.maybeFail(commit(null));
    }

    default SSTableReader onlyOne()
    {
        final Set<SSTableReader> originals = originals();
        assert originals.size() == 1;
        return Iterables.getFirst(originals, null);
    }
}
