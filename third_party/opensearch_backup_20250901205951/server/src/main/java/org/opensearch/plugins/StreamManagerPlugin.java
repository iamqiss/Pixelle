/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugins;

import org.density.arrow.spi.StreamManager;

import java.util.Optional;

/**
 * An interface for Density plugins to implement to provide a StreamManager.
 * Plugins can implement this interface to provide custom StreamManager implementation
 * or get a reference to the StreamManager instance provided by Density.
 *
 * @see StreamManager
 */
public interface StreamManagerPlugin {
    /**
     * Returns the StreamManager instance for this plugin.
     *
     * @return The StreamManager instance
     */
    default Optional<StreamManager> getStreamManager() {
        return Optional.empty();
    }

    /**
     * Called when the StreamManager is initialized.
     * @param streamManager Supplier of the StreamManager instance
     */
    default void onStreamManagerInitialized(StreamManager streamManager) {}
}
