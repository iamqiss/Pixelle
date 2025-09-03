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
package org.neo4j.util.concurrent;

import java.util.concurrent.ExecutionException;

/**
 * The past or future application of work submitted asynchronously to a {@link WorkSync}.
 */
public interface AsyncApply {
    /**
     * Await the application of the work submitted to a {@link WorkSync}.
     * <p>
     * If the work is already done, then this method with return immediately.
     * <p>
     * If the work has not been done, then this method will attempt to grab the {@code WorkSync} lock to complete the
     * work, or block to wait for another thread to complete the work on behalf of the current thread.
     *
     * @throws ExecutionException if this thread ends up performing the work, and an exception is thrown from the
     * attempt to apply the work.
     */
    void await() throws ExecutionException;

    /**
     * Tries to complete application of the work submitted to a {@link WorkSync}.
     * <p>
     * If the work is already done, then this method returns {@code true} immediately.
     * <p>
     * If the work has not been done, then this method will attempt to grab the {@code WorkSync} lock to complete the
     * work, or if it couldn't grab the lock return {@code false} whereby the caller of this method will have
     * to call either {@link #tryComplete()} or {@link #await()} at a later point to ensure the work gets completed.
     * @return {@code true} if the work is already done, or was completed as part of this call, otherwise
     * {@code false}.
     * @throws ExecutionException if this thread ends up performing the work, and an exception is thrown from the
     * attempt to apply the work.
     */
    boolean tryComplete() throws ExecutionException;

    AsyncApply EMPTY = new AsyncApply() {
        @Override
        public void await() {}

        @Override
        public boolean tryComplete() {
            return true;
        }
    };
}
