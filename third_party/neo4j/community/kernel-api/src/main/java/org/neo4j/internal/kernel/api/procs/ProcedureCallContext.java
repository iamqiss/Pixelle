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
package org.neo4j.internal.kernel.api.procs;

import java.util.stream.Stream;
import org.neo4j.memory.MemoryTracker;

/**
 * This class captures information about the context in which a procedure was called. For example if it was called within Cypher it might
 * have a YIELD clause with only a few of the available fields requested, in which case procedure authors can optimize their procedures to
 * skip calculating and returning the unused fields.
 */
public class ProcedureCallContext {

    public static String[] EMPTY_OUTPUT_FIELDNAMES = new String[0];

    private final int id;
    private final String[] outputFieldNames;
    private final boolean calledFromCypher;
    private final String database;
    private final boolean isSystemDatabase;
    private final MemoryTracker memoryTracker;

    private final String runtimeUsed;

    public ProcedureCallContext(
            int id,
            boolean calledFromCypher,
            String database,
            boolean isSystemDatabase,
            String runtimeUsed,
            MemoryTracker memoryTracker) {
        this(id, EMPTY_OUTPUT_FIELDNAMES, calledFromCypher, database, isSystemDatabase, runtimeUsed, memoryTracker);
    }

    public ProcedureCallContext(
            int id,
            String[] outputFieldNames,
            boolean calledFromCypher,
            String database,
            boolean isSystemDatabase,
            String runtimeUsed,
            MemoryTracker memoryTracker) {
        this.id = id;
        this.outputFieldNames = outputFieldNames;
        this.calledFromCypher = calledFromCypher;
        this.database = database;
        this.isSystemDatabase = isSystemDatabase;
        this.runtimeUsed = runtimeUsed;
        this.memoryTracker = memoryTracker;
    }

    /*
     * Get a stream of all the field names the procedure was requested to yield
     */
    public Stream<String> outputFields() {
        return Stream.of(outputFieldNames);
    }

    /*
     * Indicates whether the procedure was called via a complete Cypher stack.
     * Check this to make sure you are not in a testing environment.
     * When this is false, we cannot make use of the information in outputFields().
     */
    public boolean isCalledFromCypher() {
        return calledFromCypher;
    }

    public String databaseName() {
        return database;
    }

    public boolean isSystemDatabase() {
        return isSystemDatabase;
    }

    /* should only be used for testing purposes */
    public static final ProcedureCallContext EMPTY =
            new ProcedureCallContext(-1, EMPTY_OUTPUT_FIELDNAMES, false, "", false, "", null);

    public int id() {
        return id;
    }

    public String cypherRuntimeName() {
        return runtimeUsed;
    }

    /*
     * This memory tracker is NOT intended to be used directly from procedures.
     * Instead, prefer to inject org.neo4j.procedure.memory.ProcedureMemory and access trackers from there.
     * Returns a memory tracker that is bound to the executing query,
     * that can be used to record allocations inside procedures.
     */
    public MemoryTracker memoryTracker() {
        return memoryTracker;
    }
}
