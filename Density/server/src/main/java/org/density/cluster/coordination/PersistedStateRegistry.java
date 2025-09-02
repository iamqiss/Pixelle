/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.coordination;

import org.density.cluster.coordination.CoordinationState.PersistedState;
import org.density.common.util.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A class which encapsulates the PersistedStates
 *
 * @density.internal
 */
public class PersistedStateRegistry implements Closeable {

    public PersistedStateRegistry() {}

    /**
     * Distinct Types PersistedState which can be present on a node
     */
    public enum PersistedStateType {
        LOCAL,
        REMOTE;
    }

    private final Map<PersistedStateType, PersistedState> persistedStates = new ConcurrentHashMap<>();

    public void addPersistedState(PersistedStateType persistedStateType, PersistedState persistedState) {
        PersistedState existingState = this.persistedStates.putIfAbsent(persistedStateType, persistedState);
        assert existingState == null : "should only be set once, but already have " + existingState;
    }

    public PersistedState getPersistedState(PersistedStateType persistedStateType) {
        return this.persistedStates.get(persistedStateType);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(persistedStates.values());
    }

}
