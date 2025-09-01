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
package org.neo4j.internal.kernel.api.helpers.traversal.ppbfs;

import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks;
import org.neo4j.memory.MemoryTracker;

public interface TraversalMatchModeFactory {

    SignpostTracking twoWaySignpostTracking();

    Lengths lengths();

    MREValidator mreValidator();

    static TraversalMatchModeFactory trailMode(MemoryTracker memoryTracker, PPBFSHooks hooks) {
        return new TraversalMatchModeFactory() {
            @Override
            public SignpostTracking twoWaySignpostTracking() {
                return SignpostTracking.trailMode(memoryTracker, hooks);
            }

            @Override
            public Lengths lengths() {
                return Lengths.trailMode();
            }

            @Override
            public MREValidator mreValidator() {
                return MREValidator.TRAIL_MODE;
            }
        };
    }

    static TraversalMatchModeFactory walkMode() {
        return new TraversalMatchModeFactory() {
            @Override
            public SignpostTracking twoWaySignpostTracking() {
                return SignpostTracking.walkMode();
            }

            @Override
            public Lengths lengths() {
                return Lengths.walkMode();
            }

            @Override
            public MREValidator mreValidator() {
                return MREValidator.WALK_MODE;
            }
        };
    }
}
